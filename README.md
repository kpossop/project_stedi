# Documentación del Proyecto STEDI: Solución de Data Lake en AWS

## Descripción General

Este proyecto se centró en la construcción de una solución de Data Lake utilizando AWS Glue, Spark y el framework de Hudi para procesar y catalogar datos provenientes de sensores y aplicaciones móviles del dispositivo STEDI Step Trainer. Se crearon tres bases de datos de catálogos en AWS Glue, cada una correspondiente a una de las zonas del Data Lake: landing, trusted y curated.

## Estructura de las Bases de Datos de Catálogo

1. **stedi_landing**: Esta base de datos almacena los datos crudos que llegan al Data Lake en su forma original. La información se catalogó utilizando Crawlers de AWS Glue, lo que permitió identificar automáticamente los esquemas de los datos provenientes de los sensores y aplicaciones móviles.

2. **stedi_trusted**: Esta base de datos contiene los datos que han sido filtrados y procesados para garantizar su calidad. Los registros de clientes y lecturas del acelerómetro se incluyeron solo si los usuarios aceptaron compartir sus datos para investigación. La información fue catalogada y escrita con el framework Hudi en un Glue Job, lo que facilitó la escritura incremental y las actualizaciones eficientes.

3. **stedi_curated**: Esta base de datos almacena los datos completamente procesados y listos para su análisis y uso en modelos de aprendizaje automático. Se creó y catalogó mediante un Glue Job que también hizo uso de Hudi para garantizar la sincronización y gestión eficiente de los datos en el catálogo.

## Ventajas de Utilizar Glue Jobs y Hudi

### Ventajas de Glue Jobs
Los Glue Jobs proporcionan un entorno flexible y controlado para desarrollar y ejecutar procesos de ETL (Extract, Transform, Load) en Python y Spark. A diferencia de Glue Studio, permiten una personalización más avanzada y un código modular que facilita la reutilización y mantenimiento, especialmente en proyectos complejos.

### Ventajas de Hudi
Hudi (Hadoop Upsert Delete Incremental) es un framework que permite realizar escrituras incrementales y actualizaciones en los data lakes. Sus principales beneficios incluyen:
- **Actualizaciones y escrituras incrementales**: Permite actualizar registros sin necesidad de rescribir toda la partición.
- **Gestión de versiones**: Facilita la consulta de versiones históricas de los datos, lo cual es útil para auditorías.
- **Sincronización con catálogos**: Hudi se integra de manera eficiente con catálogos como Glue y Hive, manteniendo las tablas actualizadas automáticamente.

## Estructura del Proyecto
```plaintext
├── README.md
└── src
    ├── curated_zone
    │   └── stedi_tables_curated_dev.py  # Script para procesar datos de la zona trusted a la zona curated.
    ├── ddl_landing
    │   ├── accelerometer_landing.sql    # Script SQL para definir la tabla de accelerometer landing.
    │   ├── customer_landing.sql         # Script SQL para definir la tabla de customer landing.
    │   └── step_trainer_landing.sql     # Script SQL para definir la tabla de step trainer landing.
    ├── images
    │   ├── curated_zone
    │   │   ├── Job Curated.png               # Captura de pantalla del job de la zona curated en Glue.
    │   │   ├── customer_curated-count.png    # Imagen mostrando el conteo de registros en la tabla customer curated.
    │   │   ├── customer_curated-data.png     # Imagen mostrando ejemplos de datos en la tabla customer curated.
    │   │   ├── customer_trusted-sharewithresearchasofdate.png  # Representación visual de los datos filtrados de clientes.
    │   │   ├── machine_learning_curated-count.png  # Conteo de registros en la tabla machine learning curated.
    │   │   └── machine_learning_curated-data.png   # Ejemplos de datos en la tabla machine learning curated.
    │   ├── landing_zone
    │   │   ├── Crawlers Landing.png                # Captura de pantalla de los Crawlers de la zona de landing en Glue.
    │   │   ├── accelerometer_landing-count.png     # Conteo de registros en la tabla de accelerometer landing.
    │   │   ├── accelerometer_landing.png           # Ejemplo de datos de la tabla de accelerometer landing.
    │   │   ├── customer_landing-count.png          # Conteo de registros en la tabla de customer landing.
    │   │   ├── customer_landing.png                # Ejemplo de datos de la tabla de customer landing.
    │   │   ├── step_trainer_landing-count.png      # Conteo de registros en la tabla de step trainer landing.
    │   │   └── step_trainer_landing.png            # Ejemplo de datos de la tabla de step trainer landing.
    │   ├── s3_bucket
    │   │   ├── accelerometer.png                   # Estructura del bucket S3 para los datos de accelerometer.
    │   │   ├── customer.png                        # Estructura del bucket S3 para los datos de customer.
    │   │   └── step_trainer.png                    # Estructura del bucket S3 para los datos de step trainer.
    │   └── trusted_zone
    │       ├── Job Trusted.png                     # Captura de pantalla del job de la zona trusted en Glue.
    │       ├── accelerometer_trusted-count.png     # Conteo de registros en la tabla de accelerometer trusted.
    │       ├── accelerometer_trusted-data.png      # Ejemplo de datos de la tabla de accelerometer trusted.
    │       ├── customer_trusted-count.png          # Conteo de registros en la tabla de customer trusted.
    │       ├── customer_trusted-data.png           # Ejemplo de datos de la tabla de customer trusted.
    │       ├── step_trainer_trusted-count.png      # Conteo de registros en la tabla de step trainer trusted.
    │       └── step_trainer_trusted-data.png       # Ejemplo de datos de la tabla de step trainer trusted.
    └── trusted_zone
        ├── accelerometer_landing_to_trusted.py     # Script para procesar datos de accelerometer de landing a trusted.
        ├── customer_landing_to_trusted.py          # Script para procesar datos de customer de landing a trusted.
        └── step_trainer_trusted.py                 # Script para procesar datos de step trainer de landing a trusted.
```
## Descripción de Resultados
Las imágenes muestran que los datos procesados en las zonas trusted y curated cumplen con los criterios del proyecto. Los conteos de registros obtenidos son los esperados, validando que los procesos de ETL implementados con Glue Jobs y el uso de Hudi han sido exitosos y eficientes.

Clientes curados: 482 registros.
Lecturas de machine learning curadas: 43,681 registros.