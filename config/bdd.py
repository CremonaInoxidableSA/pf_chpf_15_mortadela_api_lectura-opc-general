from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError
from dotenv import load_dotenv
import os
import pymysql
import time
import threading
import logging
from sqlalchemy.exc import OperationalError

load_dotenv()

db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST", "localhost")
db_port = os.getenv("DB_PORT", "3306")
db_name = os.getenv("DB_NAME")

DATABASE_URL = f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

print(f"Usuario: {db_user}, Password: {db_password}, Host: {db_host}, Puerto: {db_port}, DB: {db_name}")

engine = None
engine_lock = threading.Lock()

def try_connect(db_url):
    print(f"DATO ULR: {db_url}")
    attempt = 1
    
    while True:
        try:
            test_engine = create_engine(db_url)
            with test_engine.connect() as connection:
                print(f"Conectado a la base de datos en {db_url}")
                return test_engine
        except OperationalError as e:
            print(f"Intento {attempt} - Error al conectar a la base de datos: {e}")
            print("Reintentando conexión en 3 segundos...")
            time.sleep(3)
            attempt += 1

def check_connection():
    """Verifica si la conexión está activa"""
    global engine
    try:
        if engine:
            with engine.connect() as connection:
                connection.execute(text("SELECT 1"))
                return True
    except Exception as e:
        print(f"Error en verificación de conexión: {e}")
        return False
    return False

def reconnect():
    """Función para reconectar cuando se detecta una falla"""
    global engine
    print("Detectada pérdida de conexión. Iniciando reconexión...")
    
    with engine_lock:
        engine = try_connect(DATABASE_URL)
        
        if not engine:
            print("Intentando conexión con localhost...")
            local_db_host = "localhost"
            local_DATABASE_URL = f"mysql+pymysql://{db_user}:{db_password}@{local_db_host}:{db_port}/{db_name}"
            engine = try_connect(local_DATABASE_URL)
        
        global SessionLocal
        SessionLocal.configure(bind=engine)

def connection_monitor():
    """Monitor que verifica la conexión cada 10 segundos"""
    while True:
        time.sleep(10)
        if not check_connection():
            reconnect()

# Conexión inicial
engine = try_connect(DATABASE_URL)

if not engine:
    print("Intentando conexión con localhost...")
    db_host = "localhost"
    DATABASE_URL = f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    engine = try_connect(DATABASE_URL)

if not engine:
    raise Exception("No se pudo conectar a la base de datos ni con el valor de la variable de entorno ni con localhost.")

monitor_thread = threading.Thread(target=connection_monitor, daemon=True)
monitor_thread.start()
print("Monitor de conexión iniciado...")

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_session_local():
    """Función para obtener SessionLocal actualizado"""
    global engine
    with engine_lock:
        return sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db():
    SessionLocal = get_session_local()
    db = SessionLocal()
    try:
        yield db
    except Exception as e:
        db.rollback()
        raise
    finally:
        db.close()


# ============== FUNCIONES PARA GESTIÓN DE CICLOS ==============
# Nota: La tabla 'ciclos' es gestionada por otra API. 
# Estas funciones solo leen/escriben en la tabla existente (sin crear).

def create_ciclo_sync(fecha_inicio, id_receta, id_torre):
    """Crea un nuevo ciclo en la tabla ciclos (existente) y retorna el id_ciclo creado."""
    from datetime import datetime
    
    SessionLocal = get_session_local()
    db = SessionLocal()
    try:
        query = text("""
            INSERT INTO ciclos (fecha_inicio, id_receta, id_torre, id_estado, activo)
            VALUES (:fecha_inicio, :id_receta, :id_torre, 1, 1)
        """)
        result = db.execute(query, {
            "fecha_inicio": fecha_inicio,
            "id_receta": id_receta,
            "id_torre": id_torre
        })
        db.commit()
        
        new_id = result.lastrowid
        logging.info(f"✓ Ciclo creado: id={new_id}, receta={id_receta}, torre={id_torre}, activo=1")
        return new_id
    except Exception as e:
        db.rollback()
        logging.error(f"✗ Error creando ciclo: {e}")
        raise
    finally:
        db.close()


def close_ciclo_sync(id_ciclo, fecha_fin=None):
    """Cierra un ciclo abierto escribiendo la fecha_fin, calculando tiempo_ciclo y estableciendo estado = FINALIZADO."""
    from datetime import datetime
    
    if fecha_fin is None:
        fecha_fin = datetime.now()
    
    SessionLocal = get_session_local()
    db = SessionLocal()
    try:
        # UPDATE que calcula tiempo_ciclo = TIMESTAMPDIFF(SECOND, fecha_inicio, fecha_fin)
        query = text("""
            UPDATE ciclos 
            SET fecha_fin = :fecha_fin, 
                id_estado = 1,
                tiempo_ciclo = TIMESTAMPDIFF(SECOND, fecha_inicio, :fecha_fin)
            WHERE id_ciclo = :id_ciclo
        """)
        result = db.execute(query, {
            "fecha_fin": fecha_fin,
            "id_ciclo": id_ciclo
        })
        db.commit()
        
        if result.rowcount > 0:
            # Obtener el tiempo_ciclo calculado para logging
            select_query = text("SELECT tiempo_ciclo FROM ciclos WHERE id_ciclo = :id_ciclo")
            select_result = db.execute(select_query, {"id_ciclo": id_ciclo}).fetchone()
            tiempo_ciclo = select_result[0] if select_result else "?"
            logging.info(f"✓ Ciclo cerrado: id={id_ciclo}, fin={fecha_fin.isoformat()}, tiempo_ciclo={tiempo_ciclo}s")
        else:
            logging.warning(f"✗ Ciclo no encontrado: id={id_ciclo}")
    except Exception as e:
        db.rollback()
        logging.error(f"✗ Error cerrando ciclo: {e}")
        raise
    finally:
        db.close()


def get_open_ciclo_sync():
    """Obtiene el ciclo abierto (estado=1) más reciente, si existe."""
    SessionLocal = get_session_local()
    db = SessionLocal()
    try:
        query = text("""
            SELECT * FROM ciclos 
            WHERE id_estado = 1 
            ORDER BY id_ciclo DESC 
            LIMIT 1
        """)
        result = db.execute(query)
        row = result.fetchone()
        return row if row else None
    except Exception as e:
        logging.error(f"✗ Error obteniendo ciclo abierto: {e}")
        return None
    finally:
        db.close()


# ============== FUNCIONES PARA FALLOS DE LECTURA DE CICLOS ==============

def create_fallo_captura_sync(fecha):
    """Registra un nuevo fallo de captura de ciclos en la tabla fallocapturaciclos."""
    from datetime import datetime
    
    if fecha is None:
        fecha = datetime.now()
    
    SessionLocal = get_session_local()
    db = SessionLocal()
    try:
        query = text("""
            INSERT INTO fallocapturaciclos (fecha)
            VALUES (:fecha)
        """)
        result = db.execute(query, {
            "fecha": fecha
        })
        db.commit()
        
        new_id = result.lastrowid
        logging.info(f"✓ Fallo de captura registrado: id={new_id}, fecha={fecha.isoformat()}")
        return new_id
    except Exception as e:
        db.rollback()
        logging.error(f"✗ Error registrando fallo de captura: {e}")
        raise
    finally:
        db.close()