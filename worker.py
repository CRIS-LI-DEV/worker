import paho.mqtt.client as mqtt
import requests
import json

# Configuración del broker MQTT
broker = "crossover.proxy.rlwy.net"
port = 57689
topic = "sensor/datos"  # Asume que este es el topic al que te suscribes

# URL de la API (ajusta la URL según tu entorno de desarrollo)
url = "https://viz1-production.up.railway.app/br-in/"

# Función para enviar los datos a la API Django
def enviar_datos_api(datos):
    headers = {'Content-Type': 'application/json'}
    try:
        response = requests.post(url, data=json.dumps(datos), headers=headers)
        print("Respuesta de la API:", response.json())
    except requests.exceptions.RequestException as e:
        print(f"Error al hacer la solicitud a la API: {e}")

# Esta función se ejecuta cuando el cliente MQTT recibe un mensaje
def on_message(client, userdata, message):
    try:
        # Decodificar el mensaje recibido como JSON
        datos = json.loads(message.payload.decode("utf-8"))
        
        # Imprimir los datos recibidos
        print("Datos recibidos del broker MQTT:", datos)

        # Enviar los datos a la API Django
        enviar_datos_api(datos)
    except json.JSONDecodeError:
        print("Error: El mensaje recibido no es un JSON válido")
    except Exception as e:
        print(f"Error al procesar el mensaje: {e}")

# Función de conexión exitosa
def on_connect(client, userdata, flags, rc):
    print("Conectado con el código de resultado: " + str(rc))
    # Suscribirse al topic después de la conexión
    client.subscribe(topic)

# Función principal para configurar el cliente MQTT y mantener la conexión
def main():
    # Crear un cliente MQTT
    client = mqtt.Client()

    # Configurar las funciones de callback
    client.on_connect = on_connect
    client.on_message = on_message

    # Conectar al broker MQTT
    client.connect(broker, port, 60)

    # Bucle para escuchar los mensajes
    client.loop_start()

    # Mantener el script corriendo
    try:
        while True:
            pass
    except KeyboardInterrupt:
        print("Conexión cerrada")
        client.loop_stop()

# Ejecutar el script solo si es el archivo principal
if __name__ == "__main__":
    main()
