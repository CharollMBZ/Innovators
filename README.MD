#Paso:1 Redirigete a la carpeta del proyecto con CD, si está en MacOS debe usar "/"
 
#Paso 2: Cree el ambiente virtual
python -m venv myenv

#Paso 3: Activar el entorno virtual
myenv\Scripts\activate

#Observacion: si está en MacOS debe utilizar el siguiente comando para el paso 3
source myenv/bin/activate

#Instalar las librerias del archivo requirements.txt
pip install -r requirements.txt

