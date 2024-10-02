from flask import request, abort
import secrets

# Variable global para almacenar el token dinámico
TOKEN_VALIDO = None

# Middleware para verificar el token en cada solicitud
def verificar_token():
    token = request.headers.get('Authorization')
    if not token or token != TOKEN_VALIDO:
        abort(401, description="Token no válido o faltante")

# Función para generar un token dinámico
def generate_token():
    global TOKEN_VALIDO
    TOKEN_VALIDO = secrets.token_hex(16)
    return TOKEN_VALIDO
