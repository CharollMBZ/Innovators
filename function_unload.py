import pandas as pd
import requests
from bs4 import BeautifulSoup
import pg8000

def get_html(fund,type_url):

    url = f"https://www.cmfchile.cl/institucional/mercados/entidad.php?mercado=V&rut={fund}&grupo=&tipoentidad=RGFMU&row=AAAw%20cAAhAABQKHAAN&vig=VI&control=svs&pestania={type_url}"

    try:
        headers = {
            "User-Agent": "Mozilla/5.0"  # Agrega un agente de usuario para simular un navegador web
        }

        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')  # Analiza el contenido HTML
            
            # Encuentra el div con la clase "consulta_entidad" y el atributo "id" igual a "contenido" del html
            div_consulta_entidad = soup.find("div", {"class": "consulta_entidad", "id": "contenido"})
            
            if div_consulta_entidad:
                # Obtiene el contenido del div
                contenido = div_consulta_entidad.prettify()
                return get_table(contenido)
            else:
                print("No se encontró el div con clase 'consulta_entidad' y atributo 'id' igual a 'contenido'.")
        else:
            print("Error al obtener HTML. Código de estado:", response.status_code)
    except requests.exceptions.RequestException as e:
        print("Error al obtener HTML:", e)

    return None

def get_table(html_text):
    # Analizar el texto HTML
    soup = BeautifulSoup(html_text, 'html.parser')
    # Encontrar la tabla dentro del div
    tabla = soup.find('table')
    # Convertir la tabla a un DataFrame
    df = pd.read_html(str(tabla))[0]

    return df

def transform_df_serie(df,fund):
    
    nuevos_nombres = {
    'Serie': 'serie',
    'Característica': 'caracteristica',
    'Fecha Inicio': 'fecha_inicio',
    'Fecha Término': 'fecha_termino',
    'Valor inicial cuota': 'valor_cuota_inicial',
    'Continuadora de serie': 'continuadora_serie'
    }

    df = df.rename(columns=nuevos_nombres)
    # Cambiar el tipo de serie y caracteristica a texto
    df['serie'] = df['serie'].astype(str)
    df['caracteristica'] = df['caracteristica'].astype(str)

    # Cambiar el tipo de fecha_inicio y fecha_termino a fecha sin hora
    df['fecha_inicio'] = pd.to_datetime(df['fecha_inicio'],format='%d/%m/%Y', errors='coerce').dt.date
    df['fecha_termino'] = pd.to_datetime(df['fecha_termino'],format='%d/%m/%Y', errors='coerce').dt.date

    # Cambiar el tipo de valor_cuota_inicial a valor numérico
    df['valor_cuota_inicial'] = df['valor_cuota_inicial'].astype(float)

    # Cambiar el tipo de continuadora_serie a texto
    df['continuadora_serie'] = df['continuadora_serie'].astype(str) 
    df['run_fm']=fund

    df=transform_null(df)

    return df

def transform_df_detalle_fondo(df, fund):
    # Crear un DataFrame a partir de la primera fila de df
    df_aux = pd.DataFrame(columns=df[0].tolist())
    df_aux.loc[0] = df[1].tolist()

    # Diccionario de nuevos nombres de columnas
    nuevos_nombres = {
        'R.U.N. Fondo Mutuo': 'run_fondo_largo',
        'Nombre Fondo Mutuo': 'nombre_fondo',
        'Nombre Corto': 'nombre_fm_corto',
        'Vigencia': 'vigencia',
        'Estado (indica si fondo está liquidado)': 'estado',
        'Tipo de Fondo Mutuo': 'tipo_fondo',
        'R.U.T. Administradora': 'rut_adm',
        'Razón Social Administradora': 'razon_social_adm',
        'Fecha Depósito Fondo Mutuo': 'fecha_deposito',
        'Fecha Ultima Modificación': 'fecha_ult_modificacion',
        'Fecha Inicio Operaciones': 'fecha_inicio_operaciones',
        'Nro. y Fecha de Resolución Aprobatoria': 'n_resolucion',
        'Fecha cumplimiento, art. 11 D.L 1.328': 'fecha_cumplimiento',
        'Fecha Término Operaciones': 'fecha_termino',
        'Número de Registro': 'numero_registro'
    }

    # Renombrar las columnas en el DataFrame
    df_aux.rename(columns=nuevos_nombres, inplace=True)

    # Establecer el valor 'run_fm' con el valor de 'fund'
    df_aux['run_fm'] = fund

    # Aplicar el formato de fecha a las columnas necesarias
    date_columns = ['fecha_deposito', 'fecha_ult_modificacion', 'fecha_inicio_operaciones',
                    'fecha_cumplimiento', 'fecha_termino']
    for column in date_columns:
        df_aux[column] = pd.to_datetime(df_aux[column], format='%d/%m/%Y', errors='coerce').dt.date

    # Transformar NaN en None
    df_aux = transform_null(df_aux)

    return df_aux

def transform_null(df):
    # Reemplaza los valores nulos en el DataFrame con None
    df = df.replace({pd.NaT: None})
    # Reemplazamos NaN por None en el DataFrame
    df = df.replace("nan", None)
    df = df.replace("None", None)
    # Reemplazar todos los valores NaN por None en el DataFrame
    df = df.where(pd.notna(df), None)
    numeric_columns = df.select_dtypes(include='float64').columns
    df[numeric_columns] = df[numeric_columns].applymap(lambda x: None if pd.isna(x) else x)
    return df


def insert_tb_series(df,connection):
        # Iterar a través de las filas del DataFrame e insertar en la tabla
     for index, row in df.iterrows():
         # Aquí debe modificar, asegúrate de que los nombres de las columnas coincidan con la tabla
         sql_query = "INSERT INTO series (run_fm, serie, caracteristica, fecha_inicio, fecha_termino, valor_cuota_inicial, continuadora_serie) VALUES (%s, %s, %s, %s, %s, %s, %s)"  # Reemplaza con los nombres de tus columnas
         # Ejecutar la inserción con los valores de la fila actual
         connection.cursor().execute(sql_query, (row['run_fm'], row['serie'], row['caracteristica'], row['fecha_inicio'], row['fecha_termino'], row['valor_cuota_inicial'], row['continuadora_serie']))
         # Confirmar la transacción
         connection.commit()

def insert_tb_detalle_fondo(df,connection):
            # Iterar a través de las filas del DataFrame e insertar en la tabla
    for index, row in df.iterrows():
        # Aquí debe modificar, asegúrate de que los nombres de las columnas coincidan con la tabla
        sql_query = "INSERT INTO detalle_fondo (run_fm,run_fondo_largo,nombre_fondo,nombre_fm_corto,vigencia,estado,tipo_fondo,rut_adm,razon_social_adm,fecha_deposito,fecha_ult_modificacion,fecha_inicio_operaciones,n_resolucion,fecha_cumplimiento,fecha_termino,numero_registro) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"  # Reemplaza con los nombres de tus columnas
        # Ejecutar la inserción con los valores de la fila actual
        connection.cursor().execute(sql_query, (row['run_fm'],row['run_fondo_largo'],row['nombre_fondo'],row['nombre_fm_corto'],row['vigencia'],row['estado'],row['tipo_fondo'],row['rut_adm'],row['razon_social_adm'],row['fecha_deposito'],row['fecha_ult_modificacion'],row['fecha_inicio_operaciones'],row['n_resolucion'],row['fecha_cumplimiento'],row['fecha_termino'],row['numero_registro']))
        # Confirmar la transacción
