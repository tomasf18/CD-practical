from src.client import Client

if __name__ == "__main__":
    c = Client("Foo") # Foo é argmento para a classe Client
    c.connect()
    
    c.loop()