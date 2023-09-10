## Descripción de cada etapa para el desarrollo del Data Warehouse (DW) 🚀

| Etapa | Descripción |
|-------|-------------|
| Definición de fuentes de datos | En esta etapa, se identificarán todas las fuentes de datos necesarias para el análisis y modelos de recomendación. Entre ellas, destacan Yelp, Google Maps y otras fuentes relevantes. Se planificará cómo obtener y procesar estos datos para alimentar el Data Warehouse. |
| Desarrollo del DW en Local | Se procederá a configurar y diseñar una base de datos local utilizando MongoDB para almacenar el Data Warehouse. Para organizar los datos, se crearán collections específicas para cada fuente de datos, como Yelp y Google Maps. Se empleará un modelo estrella para optimizar las relaciones entre tablas y facilitar el acceso a los datos. |
| Pruebas y Optimización | En esta fase, se realizarán pruebas exhaustivas para asegurar el correcto funcionamiento del Data Warehouse en la configuración local. Además, se optimizarán consultas y estructuras para mejorar el rendimiento y eficiencia del DW. |
| Migración a la Nube de Google | Se preparará el entorno en Google Cloud Platform (GCP) para la migración del DW. La base de datos local con MongoDB será migrada a la nube de Google utilizando las herramientas adecuadas para MongoDB en Google Cloud. |
| Automatización del ETL | El proceso de Extracción, Transformación y Carga (ETL) será estandarizado. Se utilizarán las herramientas disponibles en Google Cloud Platform para automatizar el flujo de trabajo de ETL. Asimismo, se implementará un orquestador de flujos de trabajo para programar y ejecutar automáticamente las cargas de datos. |
| Carga Incremental de Datos Nuevos | Se diseñará un proceso de carga incremental que detecte y cargue automáticamente los nuevos datos provenientes de Yelp, Google Maps y otras fuentes en el Data Warehouse. Esto permitirá mantener actualizada la información en el DW. |
| Tablas de Hechos y Dimensiones | Se definirán las tablas de hechos relacionadas con las reseñas de usuarios en Yelp y Google Maps. También, se identificarán y crearán las tablas de dimensiones para usuarios, empresas y otras variables dimensionales relevantes para el proyecto. |
| Consumo de Datos y Respuestas a Preguntas | Los datos en el Data Warehouse se estructurarán para facilitar su consumo y análisis. Se utilizarán herramientas de visualización y análisis de datos disponibles en Google Cloud Platform para responder a las preguntas e insights necesarios. |
| Monitoreo y Mantenimiento | Se establecerá un sistema de monitoreo en la nube para asegurar el rendimiento y disponibilidad del Data Warehouse. Además, se realizarán tareas de mantenimiento y actualizaciones periódicas para mantener el DW en óptimas condiciones. |



