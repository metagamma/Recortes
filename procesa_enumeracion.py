import pyodbc
import logging
import os
from datetime import datetime
from collections import defaultdict

SQLSERVER_CONFIG = {
   'driver': 'ODBC Driver 17 for SQL Server',
   'server': 'CAPTURE-SRV',
   'database': 'DIGITALIZACION',
   'uid': 'sa',
   'pwd': 'safd.2024',
   'TrustServerCertificate': 'yes'
}

def setup_logging():
    """Configura el sistema de logging."""
    # Crear el directorio de logs si no existe
    log_dir = 'logs'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # Configurar el nombre del archivo de log con la fecha
    log_filename = os.path.join(log_dir, f'procesamiento_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    
    # Configurar el logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    
    return logging.getLogger(__name__)

def connect_to_database():
    """Establece la conexión con la base de datos SQL Server."""
    try:
        conn_str = ';'.join([f"{k}={v}" for k, v in SQLSERVER_CONFIG.items()])
        conexion = pyodbc.connect(conn_str)
        logging.info("Conexión exitosa a SQL Server")
        return conexion
    except pyodbc.Error as e:
        logging.error(f"Error al conectar a la base de datos: {str(e)}")
        return None

def get_file_groups(connection):
    """
    Agrupa los archivos por directorio para procesarlos secuencialmente.
    Solo considera el código de barras C39.
    """
    groups = defaultdict(list)
    
    query = """
    SELECT Id, Ruta, BarcodeC39
    FROM Codificacion
    ORDER BY Ruta
    """
    
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        
        for row in rows:
            directory = os.path.dirname(row.Ruta)
            groups[directory].append({
                'id': row.Id,
                'ruta': row.Ruta,
                'barcode': row.BarcodeC39,
                'has_barcode': bool(row.BarcodeC39)
            })
            
        # Ordenar archivos dentro de cada grupo
        for directory in groups:
            groups[directory].sort(key=lambda x: x['ruta'])
            
        return groups
    except pyodbc.Error as e:
        logging.error(f"Error al obtener los grupos de archivos: {str(e)}")
        return None

def update_records(connection, groups):
    """
    Actualiza los números de página, código de examen y prefijo en la base de datos.
    """
    update_query = """
    UPDATE Codificacion
    SET NumeroPagina = ?,
        CodigoExamen = ?,
        Prefijo = ?
    WHERE Id = ?
    """
    
    try:
        cursor = connection.cursor()
        updates = []
        total_documents = 0
        current_barcode = None
        
        # Procesar cada grupo de archivos
        for directory, files in groups.items():
            current_page = 1
            logging.info(f"Procesando directorio: {directory}")
            
            for file in files:
                # Si encuentra un C39, actualiza el código actual y reinicia el contador
                if file['has_barcode']:
                    current_page = 1
                    current_barcode = file['barcode']
                    total_documents += 1
                    logging.info(f"Nuevo documento encontrado - Barcode C39: {current_barcode}")
                
                # Obtener código de examen y prefijo
                codigo_examen = current_barcode if current_barcode else None
                prefijo = codigo_examen[:3] if codigo_examen else None
                
                updates.append((
                    current_page,           # NumeroPagina
                    codigo_examen,          # CodigoExamen
                    prefijo,                # Prefijo
                    file['id']             # Id para WHERE
                ))
                
                logging.debug(f"Preparando actualización - ID: {file['id']}, Página: {current_page}, "
                            f"Código: {codigo_examen}, Prefijo: {prefijo}")
                
                current_page += 1
        
        # Ejecutar las actualizaciones en lotes
        logging.info(f"Ejecutando actualización masiva de {len(updates)} registros...")
        cursor.executemany(update_query, updates)
        connection.commit()
        logging.info("Actualización completada exitosamente")
        
        return len(updates), total_documents
    except pyodbc.Error as e:
        logging.error(f"Error al actualizar los registros: {str(e)}")
        connection.rollback()
        return 0, 0

def verify_updates(connection):
    """
    Verifica que las actualizaciones se realizaron correctamente.
    """
    verification_query = """
    SELECT 
        COUNT(*) as total_records,
        COUNT(NumeroPagina) as pages_updated,
        COUNT(CodigoExamen) as exam_codes_updated,
        COUNT(Prefijo) as prefixes_updated
    FROM Codificacion
    """
    
    try:
        cursor = connection.cursor()
        cursor.execute(verification_query)
        result = cursor.fetchone()
        
        logging.info("Resumen de verificación:")
        logging.info(f"- Total de registros: {result.total_records}")
        logging.info(f"- Registros con número de página: {result.pages_updated}")
        logging.info(f"- Registros con código de examen: {result.exam_codes_updated}")
        logging.info(f"- Registros con prefijo: {result.prefixes_updated}")
        
        return result
    except pyodbc.Error as e:
        logging.error(f"Error al verificar las actualizaciones: {str(e)}")
        return None

def main():
    """Función principal que ejecuta todo el proceso."""
    # Configurar logging
    logger = setup_logging()
    logger.info("Iniciando proceso de actualización de registros...")
    
    # Establecer conexión
    connection = connect_to_database()
    if not connection:
        logger.error("No se pudo establecer la conexión a la base de datos")
        return
    
    try:
        # Obtener grupos de archivos
        logger.info("Obteniendo grupos de archivos...")
        groups = get_file_groups(connection)
        if not groups:
            logger.error("No se pudieron obtener los grupos de archivos")
            return
        
        # Actualizar registros
        logger.info("Actualizando registros...")
        updated_count, total_docs = update_records(connection, groups)
        
        # Verificar actualizaciones
        logger.info("Verificando actualizaciones...")
        verify_updates(connection)
        
        logger.info("Resumen final:")
        logger.info(f"- Total de registros actualizados: {updated_count}")
        logger.info(f"- Total de documentos encontrados (con código C39): {total_docs}")
        
    except Exception as e:
        logger.error(f"Error no esperado durante el proceso: {str(e)}")
    finally:
        connection.close()
        logger.info("Conexión cerrada. Proceso finalizado.")

if __name__ == "__main__":
    main()