import os
import cv2
import numpy as np
import pyodbc
import logging
from datetime import datetime
from PIL import Image
import traceback
from typing import Dict, List, Tuple
import shutil

# Configuración de la base de datos
SQLSERVER_CONFIG = {
   'driver': 'ODBC Driver 17 for SQL Server',
   'server': 'CAPTURE-SRV',
   'database': 'DIGITALIZACION',
   'uid': 'sa',
   'pwd': 'safd.2024',
   'TrustServerCertificate': 'yes'
}

class RecortesProcessor:
    def __init__(self, sql_config: Dict, output_directory: str):
        """
        Inicializa el procesador de recortes
        
        Args:
            sql_config (Dict): Configuración de conexión a SQL Server
            output_directory (str): Directorio base donde se guardarán los recortes
        """
        self.sql_config = sql_config
        self.output_directory = output_directory
        self.connection_string = self._build_connection_string()
        self.setup_logging()
        self._create_directories()

    def _build_connection_string(self) -> str:
        """Construye el string de conexión a partir de la configuración"""
        return (
            f"DRIVER={{{self.sql_config['driver']}}};"
            f"SERVER={self.sql_config['server']};"
            f"DATABASE={self.sql_config['database']};"
            f"UID={self.sql_config['uid']};"
            f"PWD={self.sql_config['pwd']};"
            f"TrustServerCertificate={self.sql_config['TrustServerCertificate']};"
        )

    def _create_directories(self):
        """Crea los directorios necesarios para el procesamiento"""
        directories = [
            self.output_directory,
            'logs',
            os.path.join(self.output_directory, 'errores')
        ]
        for directory in directories:
            os.makedirs(directory, exist_ok=True)

    def normalize_path(self, path: str) -> str:
        try:
            path = str(path).strip()  # Agregar strip() para eliminar espacios extras
            path = path.replace('/', '\\')  # Asegurar uso consistente de backslashes
            path = os.path.normpath(path)
            
            # Mejorar manejo de rutas UNC
            if path.startswith('\\\\'):
                parts = [p for p in path.split('\\') if p]  # Eliminar elementos vacíos
                path = '\\\\' + '\\'.join(parts[1:])
                
            # Expandir el reemplazo de caracteres especiales
            replacements = {
                'á': 'a', 'é': 'e', 'í': 'i', 'ó': 'o', 'ú': 'u',
                'Á': 'A', 'É': 'E', 'Í': 'I', 'Ó': 'O', 'Ú': 'U',
                'ñ': 'n', 'Ñ': 'N',
                ' ': '_'
            }
            for old, new in replacements.items():
                path = path.replace(old, new)
                
            return path
        except Exception as e:
            self.logger.error(f"Error normalizando ruta {path}: {str(e)}")
            return path
    
    def create_hierarchical_directories(self, record: Dict) -> str:
        """
        Crea la estructura jerárquica de directorios y retorna la ruta completa
        """
        try:
            # Normalizar los componentes individuales
            operativo = self.normalize_path(record['Operativo'])
            area = self.normalize_path(record['Area'])
            cod_item = self.normalize_path(record['cod_item'])
            nombre_archivo = self.normalize_path(record['NombreArchivo'])
            
            # Crear la estructura de directorios
            dir_path = os.path.join(
                self.normalize_path(self.output_directory),
                operativo,
                area,
                cod_item
            )
            
            # Normalizar la ruta completa
            dir_path = self.normalize_path(dir_path)
            
            # Crear directorios
            os.makedirs(dir_path, exist_ok=True)
            
            # Construir y normalizar la ruta completa del archivo
            full_path = self.normalize_path(os.path.join(dir_path, nombre_archivo))
            
            self.logger.info(f"Ruta normalizada creada: {full_path}")
            return full_path
            
        except Exception as e:
            self.logger.error(f"Error creando directorios: {str(e)}")
            raise

    def update_recorte_path(self, record: Dict, full_path: str):
        """
        Actualiza la ruta del recorte en la base de datos
        
        Args:
            record (Dict): Registro del recorte
            full_path (str): Ruta completa donde se guardó el recorte
        """
        try:
            query = """
            UPDATE ListadoMuestraCodificacion 
            SET RutaRecorte = ? 
            WHERE cod_barra = ? 
            AND NumeroPagina = ? 
            AND Field_id = ?
            """
            
            with pyodbc.connect(self.connection_string) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, (full_path, record['cod_barra'], 
                                        record['NumeroPagina'], record['Field_id']))
                    conn.commit()
            
            self.logger.info(f"Actualizada ruta de recorte en BD: {full_path}")
        except Exception as e:
            self.logger.error(f"Error actualizando ruta de recorte: {str(e)}")
            raise

    def setup_logging(self):
        """Configura el sistema de logging"""
        log_directory = "logs"
        if not os.path.exists(log_directory):
            os.makedirs(log_directory)

        log_file = os.path.join(
            log_directory, 
            f'recortes_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
        )
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def get_dpi_from_image(self, image_path: str) -> Tuple[int, int]:
        """
        Obtiene el DPI de una imagen
        
        Args:
            image_path (str): Ruta de la imagen
            
        Returns:
            tuple: (dpi_x, dpi_y)
        """
        try:
            with Image.open(image_path) as img:
                dpi = img.info.get('dpi', (300, 300))
                return dpi
        except Exception as e:
            self.logger.error(f"Error al obtener DPI de {image_path}: {str(e)}")
            return (300, 300)

    def inches_to_pixels(self, inches: float, width: float = None, height: float = None, 
                        is_coordinate: bool = False, dpi: int = 300) -> int:
        """
        Convierte pulgadas a píxeles usando la misma fórmula que SQL
        
        Args:
            inches (float): Medida en pulgadas
            width (float): Ancho en pulgadas (solo necesario para coordenadas X)
            height (float): Alto en pulgadas (solo necesario para coordenadas Y)
            is_coordinate (bool): True si es una coordenada (x,y), False si es una dimensión
            dpi (int): DPI de la imagen
            
        Returns:
            int: Medida en píxeles
        """
        try:
            if is_coordinate:
                if width is not None:  # Para coordenada X
                    return int((float(inches) - float(width)/2) * dpi)
                elif height is not None:  # Para coordenada Y
                    return int((float(inches) - float(height)/2) * dpi)
                else:
                    raise ValueError("Se requiere width o height para convertir coordenadas")
            else:
                return int(float(inches) * dpi)
        except Exception as e:
            self.logger.error(f"Error en conversión de pulgadas a píxeles: {str(e)}")
            raise

    def validate_image_path(self, image_path: str) -> bool:
        """
        Valida que la ruta de la imagen exista y sea accesible
        
        Args:
            image_path (str): Ruta de la imagen a validar
            
        Returns:
            bool: True si la imagen es válida, False en caso contrario
        """
        if not os.path.exists(image_path):
            self.logger.error(f"La imagen no existe: {image_path}")
            return False
        
        if not os.access(image_path, os.R_OK):
            self.logger.error(f"No hay permisos de lectura para: {image_path}")
            return False
            
        return True

    def get_records_to_process(self) -> List[Dict]:
        """
        Obtiene los registros a procesar de la base de datos
        
        Returns:
            list: Lista de registros a procesar
        """
        query = """
        SELECT DISTINCT 
            l.Prefijo,
            l.cod_barra,
            l.NombreArchivo,
            l.NumeroPagina,
            l.Field_id,
            l.Operativo,
            l.Area,
            l.cod_item,
            c.Ruta,
            CAST(f.Cord_x AS float) as Cord_x,
            CAST(f.Cord_y AS float) as Cord_y,
            CAST(f.Cord_width AS float) as Cord_width,
            CAST(f.Cord_height AS float) as Cord_height,
            c.Id as CodificacionId
        FROM ListadoMuestraCodificacion l
        INNER JOIN Codificacion c ON (c.CodigoExamen = l.cod_barra and c.NumeroPagina = l.NumeroPagina)
        INNER JOIN Tbl_Fields f ON f.ID = l.Field_id
        WHERE l.RutaRecorte IS NULL
        ORDER BY c.Id, l.cod_barra
        """
        
        try:
            with pyodbc.connect(self.connection_string) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    columns = [column[0] for column in cursor.description]
                    return [dict(zip(columns, row)) for row in cursor.fetchall()]
        except Exception as e:
            self.logger.error(f"Error al obtener registros: {str(e)}")
            raise

    def process_image(self, record: Dict):
        """Procesa una imagen individual"""
        try:
            image_path = record['Ruta']
            self.logger.info(f"Procesando imagen: {image_path}")
            
            # Validar imagen
            if not self.validate_image_path(image_path):
                raise ValueError(f"Imagen inválida: {image_path}")
            
            # Obtener DPI de la imagen
            dpi = self.get_dpi_from_image(image_path)
            self.logger.info(f"DPI de la imagen: {dpi}")
            
            # Leer imagen con OpenCV
            img = cv2.imread(image_path)
            if img is None:
                raise ValueError(f"No se pudo cargar la imagen: {image_path}")
            
            # Convertir coordenadas de pulgadas a píxeles
            width = self.inches_to_pixels(float(record['Cord_width']), dpi=int(dpi[0]))
            height = self.inches_to_pixels(float(record['Cord_height']), dpi=int(dpi[1]))
            x = self.inches_to_pixels(float(record['Cord_x']), width=float(record['Cord_width']), 
                                    is_coordinate=True, dpi=int(dpi[0]))
            y = self.inches_to_pixels(float(record['Cord_y']), height=float(record['Cord_height']), 
                                    is_coordinate=True, dpi=int(dpi[1]))
            
            # Realizar el recorte
            crop = img[y:y+height, x:x+width]
            
            # Crear estructura de directorios y obtener ruta completa
            output_path = self.create_hierarchical_directories(record)
            output_path = self.normalize_path(output_path)
            
            # Verificar permisos antes de guardar
            output_dir = os.path.dirname(output_path)
            if not os.access(output_dir, os.W_OK):
                self.logger.warning(f"Verificando permisos en directorio: {output_dir}")
                try:
                    os.makedirs(output_dir, exist_ok=True)
                except Exception as e:
                    raise ValueError(f"No se pueden crear/acceder los directorios: {str(e)}")
            
            # Guardar como TIFF
            success = cv2.imwrite(
                output_path,
                crop,
                [
                    cv2.IMWRITE_TIFF_COMPRESSION, 5,  # Sin compresión
                    cv2.IMWRITE_TIFF_RESUNIT, 2,     # Pulgadas
                    cv2.IMWRITE_TIFF_XDPI, int(dpi[0]),   # DPI X
                    cv2.IMWRITE_TIFF_YDPI, int(dpi[1])    # DPI Y
                ]
            )
            
            if not success:
                self.logger.warning(f"Intentando guardar con PIL...")
                pil_image = Image.fromarray(cv2.cvtColor(crop, cv2.COLOR_BGR2RGB))
                pil_image.save(output_path, 'TIFF', dpi=dpi)
            
            # Verificar que el archivo se creó correctamente
            if not os.path.exists(output_path) or os.path.getsize(output_path) == 0:
                raise ValueError(f"El archivo de salida no se creó correctamente: {output_path}")
            
            # Actualizar la ruta en la base de datos
            self.update_recorte_path(record, output_path)
            
            self.logger.info(f"Recorte guardado exitosamente: {output_path}")
            self.logger.debug(f"Dimensiones del recorte: {width}x{height} píxeles")
            
        except Exception as e:
            error_msg = f"Error procesando imagen {image_path}: {str(e)}"
            self.logger.error(error_msg)
            self.logger.error(traceback.format_exc())
            self.backup_failed_image(image_path, error_msg)
            raise

    def backup_failed_image(self, image_path: str, error_info: str):
        """
        Guarda una copia de la imagen que falló junto con información del error
        
        Args:
            image_path (str): Ruta de la imagen que falló
            error_info (str): Información del error
        """
        try:
            error_dir = os.path.join(self.output_directory, 'errores')
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            error_image_path = os.path.join(
                error_dir, 
                f"error_{timestamp}_{os.path.basename(image_path)}"
            )
            error_info_path = f"{error_image_path}.txt"
            
            # Copiar imagen
            shutil.copy2(image_path, error_image_path)
            
            # Guardar información del error
            with open(error_info_path, 'w', encoding='utf-8') as f:
                f.write(f"Fecha: {datetime.now()}\n")
                f.write(f"Imagen original: {image_path}\n")
                f.write(f"Error: {error_info}\n")
                
            self.logger.info(f"Backup de imagen con error guardado en: {error_image_path}")
        except Exception as e:
            self.logger.error(f"Error al hacer backup de imagen fallida: {str(e)}")

    def process_all(self):
        """Procesa todos los recortes pendientes"""
        try:
            self.logger.info("Iniciando procesamiento de recortes")
            records = self.get_records_to_process()
            total_records = len(records)
            self.logger.info(f"Se encontraron {total_records} recortes para procesar")
            
            processed = 0
            errors = 0
            skipped = 0
            
            for record in records:
                try:
                    self.process_image(record)
                    processed += 1
                    if processed % 10 == 0:
                        self.logger.info(f"Progreso: {processed}/{total_records} ({(processed/total_records)*100:.1f}%)")
                except Exception as e:
                    errors += 1
                    self.logger.error(f"Error procesando registro {record.get('cod_barra', 'desconocido')}: {str(e)}")
                    continue
            
            # Resumen final
            self.logger.info("=== Resumen del Procesamiento ===")
            self.logger.info(f"Total de recortes: {total_records}")
            self.logger.info(f"Procesados exitosamente: {processed}")
            self.logger.info(f"Errores: {errors}")
            self.logger.info(f"Saltados: {skipped}")
            self.logger.info("==============================")
            
        except Exception as e:
            self.logger.error(f"Error en el procesamiento general: {str(e)}")
            self.logger.error(traceback.format_exc())
            raise

def main():
    try:
        # Configuración
        output_directory = r"D:\RECORTES_FINALES"
        
        # Verificar y crear directorio de salida
        os.makedirs(output_directory, exist_ok=True)
        
        # Crear y ejecutar el procesador
        processor = RecortesProcessor(SQLSERVER_CONFIG, output_directory)
        processor.process_all()
        
        return 0
        
    except Exception as e:
        print(f"Error crítico en la ejecución: {str(e)}")
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)