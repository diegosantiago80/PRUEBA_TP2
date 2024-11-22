from dotenv import load_dotenv
import os
import requests
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime


load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")



print(f"Conectando a MongoDB en: {MONGO_URI}")
print(f"Clave de OpenWeather: {OPENWEATHER_API_KEY}")


cliente = MongoClient(MONGO_URI)
base_de_datos = cliente["Argentina"]


def registrar_actividad(mensaje):
    with open("registro.txt", "a") as archivo:
        fecha_hora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        archivo.write(f"{fecha_hora} - {mensaje}\n")
        
def obtener_y_guardar_provincias():
    try:
        registrar_actividad("Iniciando la obtencion de provincias")
        url = "https://apis.datos.gob.ar/georef/api/provincias"
        respuesta = requests.get(url)
        respuesta.raise_for_status()

        provincias = respuesta.json().get("provincias", [])
        coleccion_provincias = base_de_datos["provincias"]
        coleccion_provincias.insert_many(provincias)

        registrar_actividad(f"Se obtuvieron y almacenaron {len(provincias)} provincias")
    except Exception as e:
        registrar_actividad(f"Error al obtener provincias: {e}")

def obtener_y_guardar_municipios():
    try:
        registrar_actividad("Iniciando la obtencion de municipios")
        coleccion_provincias = base_de_datos["provincias"]
        provincias = coleccion_provincias.find()
        
        coleccion_municipios = base_de_datos["municipios"]
        for provincia in provincias:
            url = f"https://apis.datos.gob.ar/georef/api/municipios?provincia={provincia['nombre']}"
            respuesta = requests.get(url)
            respuesta.raise_for_status()

            municipios = respuesta.json().get("municipios", [])
            if municipios:
                coleccion_municipios.insert_many(municipios)
                registrar_actividad(f"Se obtuvieron y almacenaron {len(municipios)} municipios de la provincia {provincia['nombre']}.")
    except Exception as e:
        registrar_actividad(f"Error al obtener municipios: {e}")

def obtener_y_guardar_clima():
    try:
        registrar_actividad("Iniciando la obtencion del clima")
        coleccion_municipios = base_de_datos["municipios"]
        municipios = coleccion_municipios.find()

        coleccion_clima = base_de_datos["clima"]
        for municipio in municipios:
            nombre_localidad = municipio["nombre"]
            url = f"https://api.openweathermap.org/data/2.5/weather"
            params = {"q": f"{nombre_localidad},AR", "appid": OPENWEATHER_API_KEY}
            respuesta = requests.get(url, params=params)
            
            if respuesta.status_code == 200:
                datos_clima = {
                    "localidad": nombre_localidad,
                    "provincia": municipio.get("provincia", {}).get("nombre", ""),
                    "clima": respuesta.json()
                }
                coleccion_clima.insert_one(datos_clima)
                registrar_actividad(f"Clima obtenido para {nombre_localidad}.")
            else:
                registrar_actividad(f"No se pudo obtener el clima para {nombre_localidad}.")
    except Exception as e:
        registrar_actividad(f"Error al obtener clima: {e}")

def consultar_por_provincia_o_localidad(termino_busqueda):
    try:
        coleccion_clima = base_de_datos["clima"]
        resultados = coleccion_clima.find({
            "$or": [
                {"provincia": termino_busqueda},
                {"localidad": termino_busqueda}
            ]
        })
        registrar_actividad(f"Consulta realizada para el termino: {termino_busqueda}.")
        return list(resultados)
    except Exception as e:
        registrar_actividad(f"Error al realizar consulta: {e}")


def main():
    try:
        registrar_actividad("Iniciando el proceso principal...")

        obtener_y_guardar_provincias()
        obtener_y_guardar_municipios()
        obtener_y_guardar_clima()

    
        resultados = consultar_por_provincia_o_localidad("Buenos Aires")
        for resultado in resultados:
            print(resultado)

        registrar_actividad("Proceso completado con exito.")
    except Exception as e:
        registrar_actividad(f"Error durante la ejecucion principal: {e}")
    finally:
        cliente.close()
        registrar_actividad("Conexion a la base de datos cerrada")

if __name__ == "__main__":
    main()
