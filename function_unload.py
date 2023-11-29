import pandas as pd
import requests
from bs4 import BeautifulSoup
import pg8000

def get_html(fund,type_url):

    url = f"https://www.cmfchile.cl/institucional/mercados/entidad.php?mercado=V&rut={fund}&grupo=&tipoentidad=RGFMU&row=AAAw%20cAAhAABQKHAAN&vig=VI&control=svs&pestania={type_url}"

    try:
        headers = {
            "User-Agent": "Edg/91.0.864.48"  # Agrega un agente de usuario para simular un navegador web
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
    'Serie': 'Serie',
    'Característica': 'Caracteristica',
    'Fecha Inicio': 'Fecha_Inicio',
    'Fecha Término': 'Fecha_Termino',
    'Valor inicial cuota': 'Valor_Inicial_cuota',
    'Continuadora de serie': 'Continuadora_Serie'
    }

    df = df.rename(columns=nuevos_nombres)
    # Cambiar el tipo de serie y caracteristica a texto
    df['Serie'] = df['Serie'].astype(str)
    df['Caracteristica'] = df['Caracteristica'].astype(str)

    # Cambiar el tipo de fecha_inicio y fecha_termino a fecha sin hora
    df['Fecha_Inicio'] = pd.to_datetime(df['Fecha_Inicio'],format='%d/%m/%Y', errors='coerce').dt.date
    df['Fecha_Termino'] = pd.to_datetime(df['Fecha_Termino'],format='%d/%m/%Y', errors='coerce').dt.date

    # Cambiar el tipo de valor_cuota_inicial a valor numérico
    df['Valor_Inicial_cuota'] = df['Valor_Inicial_cuota'].astype(float)

    # Cambiar el tipo de continuadora_serie a texto
    df['Continuadora_Serie'] = df['Continuadora_Serie'].astype(str) 
    df['run_fm']=fund

    df=transform_null(df)

    return df

def transform_df_detalle_fondo(df, fund):
    # Crear un DataFrame a partir de la primera fila de df
    df_aux = pd.DataFrame(columns=df[0].tolist())
    df_aux.loc[0] = df[1].tolist()

    # Diccionario de nuevos nombres de columnas
    nuevos_nombres = {
        'R.U.N. Fondo Mutuo': 'Run_Fondo_Mutuo',
        'Nombre Fondo Mutuo': 'Nombre_Fondo_Mutuo',
        'Nombre Corto': 'Nombre_corto',
        'Vigencia': 'Vigencia',
        'Estado (indica si fondo está liquidado)': 'Estado',
        'Tipo de Fondo Mutuo': 'Tipo_Fondo_Mutuo',
        'R.U.T. Administradora': 'Rut_Administradora',
        'Razón Social Administradora': 'Razon_Social_Administradora',
        'Fecha Depósito Fondo Mutuo': 'Fecha_Deposito_Fondo_Mutuo',
        'Fecha Ultima Modificación': 'Fecha_Ultima_Modificacion',
        'Fecha Inicio Operaciones': 'Fecha_inicio_Operacion',
        'Nro. y Fecha de Resolución Aprobatoria': 'Nro_Fecha_Resolucion_Aprobatoria',
        'Fecha cumplimiento, art. 11 D.L 1.328': 'Fecha_Cumplimiento',
        'Fecha Término Operaciones': 'Fecha_Termino_Operacion',
        'Número de Registro': 'Nro_Registro'
    }

    # Renombrar las columnas en el DataFrame
    df_aux.rename(columns=nuevos_nombres, inplace=True)

    # Establecer el valor 'run_fm' con el valor de 'fund'
    df_aux['run_fm'] = fund

    # Aplicar el formato de fecha a las columnas necesarias
    date_columns = ['Fecha_Deposito_Fondo_Mutuo', 'Fecha_Ultima_Modificacion', 'Fecha_inicio_Operacion',
                    'Fecha_Cumplimiento', 'Fecha_Termino_Operacion']
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
         sql_query = "INSERT INTO series (run_fm, Serie, Caracteristica, Fecha_Inicio, Fecha_Termino, Valor_Inicial_cuota, Continuadora_Serie) VALUES (%s, %s, %s, %s, %s, %s, %s)"  # Reemplaza con los nombres de tus columnas
         # Ejecutar la inserción con los valores de la fila actual
         connection.cursor().execute(sql_query, (row['run_fm'], row['Serie'], row['Caracteristica'], row['Fecha_Inicio'], row['Fecha_Termino'], row['Valor_Inicial_cuota'], row['Continuadora_Serie']))
         # Confirmar la transacción
         connection.commit()

def insert_tb_detalle_fondo(df,connection):
            # Iterar a través de las filas del DataFrame e insertar en la tabla
    for index, row in df.iterrows():
        # Aquí debe modificar, asegúrate de que los nombres de las columnas coincidan con la tabla
        sql_query = "INSERT INTO detalle_fondo (run_fm,Run_Fondo_Mutuo,Nombre_Fondo_Mutuo,Nombre_corto,Vigencia,Estado,Tipo_Fondo_Mutuo,Rut_Administradora,Razon_Social_Administradora,Fecha_Deposito_Fondo_Mutuo,Fecha_Ultima_Modificacion,Fecha_inicio_Operacion,Nro_Fecha_Resolucion_Aprobatoria,Fecha_Cumplimiento,Fecha_Termino_Operacion,Nro_Registro) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"  # Reemplaza con los nombres de tus columnas
        # Ejecutar la inserción con los valores de la fila actual
        connection.cursor().execute(sql_query, (row['run_fm'],row['Run_Fondo_Mutuo'],row['Nombre_Fondo_Mutuo'],row['Nombre_corto'],row['Vigencia'],row['Estado'],row['Tipo_Fondo_Mutuo'],row['Rut_Administradora'],row['Razon_Social_Administradora'],row['Fecha_Deposito_Fondo_Mutuo'],row['Fecha_Ultima_Modificacion'],row['Fecha_inicio_Operacion'],row['Nro_Fecha_Resolucion_Aprobatoria'],row['Fecha_Cumplimiento'],row['Fecha_Termino_Operacion'],row['Nro_Registro']))
        # Confirmar la transacción
        connection.commit()