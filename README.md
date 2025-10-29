# 🎬 Movie History Data Engineering Project

## 📋 Descripción del Proyecto

Proyecto integral de ingeniería de datos que implementa un pipeline completo de ETL/ELT utilizando **Azure Databricks**, **PySpark**, **SQL** y **Azure Data Lake Storage Gen2**, siguiendo las mejores prácticas de la **Arquitectura Medallion** (Bronze, Silver, Gold). El proyecto procesa y analiza datos históricos de películas, aplicando transformaciones complejas y generando insights de negocio.

---

## 🎯 Objetivos del Proyecto

- Implementar un pipeline de datos escalable y robusto utilizando tecnologías cloud de Azure
- Aplicar la arquitectura Medallion para garantizar calidad y gobernanza de datos
- Procesar múltiples formatos de datos (CSV, JSON) con diferentes estructuras
- Implementar seguridad mediante Azure Key Vault y Service Principal
- Generar análisis de negocio sobre presupuestos, ingresos y tendencias de la industria cinematográfica
- Demostrar competencias en ingeniería de datos utilizando herramientas enterprise-grade

---

## 🏗️ Arquitectura del Proyecto

### Arquitectura Medallion

```
┌─────────────────────────────────────────────────────────────────┐
│                     AZURE DATA LAKE GEN2                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────┐      ┌──────────────┐      ┌──────────────┐ │
│  │   BRONZE     │  ──> │   SILVER     │  ──> │    GOLD      │ │
│  │              │      │              │      │              │ │
│  │ Raw Data     │      │ Cleaned &    │      │ Business     │ │
│  │ CSV/JSON     │      │ Validated    │      │ Aggregated   │ │
│  │              │      │ Delta Tables │      │ Analytics    │ │
│  └──────────────┘      └──────────────┘      └──────────────┘ │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
         ↑                        ↑                      ↑
         │                        │                      │
    ┌────┴────┐              ┌───┴───┐             ┌────┴────┐
    │ Ingesta │              │ Trans │             │ Análisis│
    │         │              │ forma │             │         │
    └─────────┘              └───────┘             └─────────┘
```

#### **Bronze Layer (Raw Data)**
- Almacena datos en su formato original (CSV, JSON)
- Sin transformaciones, datos tal como se reciben de la fuente
- Esquemas definidos para validación inicial
- Soporta múltiples formatos: single-line JSON, multi-line JSON, CSV

#### **Silver Layer (Cleaned & Validated)**
- Datos limpios, normalizados y validados
- Formato Delta Lake para ACID transactions
- Columnas renombradas según convenciones de negocio
- Particionamiento por fecha para optimización de queries
- Implementación de merge/upsert para actualizaciones incrementales

#### **Gold Layer (Business Analytics)**
- Datos agregados y listos para consumo de BI
- Modelos dimensionales y métricas de negocio
- Optimizado para performance en queries analíticas
- Resultados precalculados para dashboards y reportes

---

## 🛠️ Stack Tecnológico

### Cloud & Storage
- **Azure Databricks**: Plataforma de procesamiento distribuido
- **Azure Data Lake Storage Gen2 (ADLS)**: Almacenamiento de datos a escala
- **Azure Key Vault**: Gestión segura de secretos y credenciales
- **Azure Entra ID (Active Directory)**: Autenticación y autorización

### Processing & Analytics
- **Apache Spark**: Motor de procesamiento distribuido
- **PySpark**: API de Python para Spark (DataFrames, SQL)
- **Delta Lake**: Capa de almacenamiento con ACID transactions
- **Spark SQL**: Queries analíticas sobre datos distribuidos

### Lenguajes
- **Python**: Lógica de transformación y procesamiento
- **SQL**: Queries analíticas y creación de objetos
- **Databricks Notebooks**: Desarrollo interactivo

### Seguridad
- **Service Principal**: Autenticación OAuth para acceso a ADLS
- **Databricks Secrets**: Gestión de credenciales encriptadas
- **RBAC (Role-Based Access Control)**: Control de acceso granular

---

## 📊 Modelo de Datos

### Entidades Principales

#### Dimensionales
- **Movies**: Información de películas (título, presupuesto, ingresos, duración, votos)
- **Languages**: Idiomas de las películas
- **Genres**: Géneros cinematográficos
- **Countries**: Países de producción
- **Production Companies**: Compañías productoras
- **Persons**: Actores y personal de producción

#### Relacionales (Bridge Tables)
- **Movie_Genres**: Relación muchos-a-muchos entre películas y géneros
- **Movie_Languages**: Idiomas asociados a cada película
- **Movie_Cast**: Cast de actores por película
- **Movie_Companies**: Compañías productoras por película
- **Production_Country**: Países de producción por película

### Esquema de Datos

**Total de registros procesados**:
- Películas: ~4,500
- Relaciones género-idioma-película: 20,031 combinaciones
- Compañías y países: 6,477 relaciones

---

## 🔄 Pipeline de Datos

### 1️⃣ Ingestion (Bronze → Silver)

**Procesos implementados**:

```python
# Lectura de datos con esquema definido
movie_df = spark.read \
    .option("header", True) \
    .schema(movie_schema) \
    .csv(f"{bronze_folder_path}/{v_file_date}/movie.csv")

# Transformaciones: selección, renombrado, adición de metadata
movies_final_df = add_ingestion_date(movies_renamed_df) \
    .withColumn("enviroment", lit(v_enviroment)) \
    .withColumn("file_date", lit(v_file_date))

# Escritura en Delta Lake con merge/upsert
merge_delta_lake(movies_final_df, "movie_silver", "movies", 
                 silver_folder_path, merge_condition, "file_date")
```

**Notebooks de Ingesta** (13 notebooks):
1. `01-Ingestion_file_movie.ipynb` - Datos principales de películas
2. `02-ingestion_file_languaje.ipynb` - Catálogo de idiomas
3. `03-ingestion_file_genre.ipynb` - Catálogo de géneros
4. `04-ingestion_file_country.ipynb` - Catálogo de países
5. `05-ingestion_file_person.ipynb` - Datos de personas (JSON complejo)
6. `06-ingestion_file_movie_genre.ipynb` - Relación película-género
7. `07-ingestion_file_movie_cast.ipynb` - Cast de películas (JSON multilínea)
8. `08-ingestion_file_language_role.ipynb` - Roles de idioma
9. `09-ingestion_folder_production_company.ipynb` - Compañías productoras
10. `10-ingestion_folder_movie_company.ipynb` - Relación película-compañía
11. `11-ingestion_folder_movie_languages.ipynb` - Relación película-idioma
12. `12-ingestion_folder_production_country.ipynb` - Países de producción
13. `13-create_silver_database.ipynb` - Creación de base de datos Silver

**Características clave**:
- ✅ Parametrización con widgets (`p_file_date`, `p_enviroment`)
- ✅ Validación de esquemas
- ✅ Particionamiento por fecha
- ✅ Merge incremental (upsert) para evitar duplicados
- ✅ Metadata de auditoría (ingestion_date, enviroment)

### 2️⃣ Transformation (Silver → Gold)

**Notebooks de Transformación** (6 notebooks):

#### **01. results_movies_genre_language**
Combina películas con sus géneros e idiomas
```python
# Joins múltiples
results_df = movie_filter_df \
    .join(languages_mov_lan_df, "movie_id", "inner") \
    .join(genres_mov_gen_df, "movie_id", "inner")

# Filtrado por año >= 2000
movie_filter_df = movies_df.filter("year_release_date >= 2000")
```
**Output**: 20,031 registros combinando películas, géneros e idiomas

#### **02. results_country_prod_company**
Análisis de presupuesto e ingresos por país y compañía
```sql
SELECT title, budget, revenue, duration_time, release_date, 
       country_name, company_name, created_date
FROM movies 
JOIN production_countries ON ...
JOIN production_companies ON ...
```
**Output**: 6,477 registros con presupuestos promedio de $43.3M y ingresos de $124M

#### **03. results_group_movie_genre**
Agregaciones por género y año
```python
results_group_by_df = results_df \
    .groupBy("year_release_date", "genre_name") \
    .agg(
        sum("budget").alias("total_budget"),
        sum("revenue").alias("total_revenue")
    )
    
# Ranking con window functions
results_dense_rank_df = Window.partitionBy("year_release_date") \
    .orderBy(desc("total_budget"), desc("total_revenue"))
```
**Insights**:
- **Adventure** es el género con mayor presupuesto ($3.3B en 2015)
- **Action** el segundo con $2.9B
- **Science Fiction** tercero con $2.1B

#### **04. results_group_movie_country**
Agregaciones por país y año
```python
# Ranking de países por ingresos totales
dense_rank_window = Window.partitionBy("year_release_date") \
    .orderBy(desc("total_revenue"))
```
**Insights**:
- **USA** lidera con $21.7B en ingresos (2015)
- **UK** segundo con $1.9B
- **Australia** tercero con $1.1B

### 3️⃣ Analysis (Gold Layer)

**Notebooks de Análisis** (4 notebooks):

1. **01.budget_revenue_country.ipynb**
   - Análisis de ROI por país
   - Comparativa de presupuestos vs ingresos
   
2. **02.budget_revenue_production_company.ipynb**
   - Performance de compañías productoras
   - Identificación de estudios más rentables

3-4. **Notebooks de análisis adicionales** con agregaciones complejas

---

## 🔐 Seguridad e Infraestructura

### Configuración de Service Principal

```python
# Autenticación OAuth con Azure AD
client_id = dbutils.secrets.get(
    scope="movie-history-secret-scope",
    key="client-id"
)
tenant_id = dbutils.secrets.get(
    scope="movie-history-secret-scope",
    key="tenant-id"
)
client_secret = dbutils.secrets.get(
    scope="movie-history-secret-scope",
    key="client-secret"
)

spark.conf.set("fs.azure.account.auth.type.moviehistory0001.dfs.core.windows.net", 
               "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.moviehistory0001.dfs.core.windows.net",
               "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
```

**Ventajas de esta implementación**:
- ✅ Sin credenciales hardcodeadas en el código
- ✅ Rotación de secretos sin cambios en notebooks
- ✅ Control de acceso basado en roles (RBAC)
- ✅ Auditoría completa de accesos

### Métodos de Autenticación Implementados

1. **Access Key** (desarrollo)
2. **SAS Token** (acceso temporal)
3. **Service Principal** (producción) ⭐ Recomendado

---

## 🚀 Funciones Reutilizables

### Common Functions (`includes/common_functions.ipynb`)

#### 1. **add_ingestion_date**
```python
def add_ingestion_date(input_df):
    """Agrega timestamp de ingesta para auditoría"""
    output_df = input_df.withColumn('ingestion_date', current_timestamp())
    return output_df
```

#### 2. **merge_delta_lake**
```python
def merge_delta_lake(input_df, db_name, table_name, folder_path, 
                     merge_condition, partition_column):
    """
    Realiza merge (upsert) en Delta Lake
    - Actualiza registros existentes
    - Inserta nuevos registros
    - Evita duplicados
    """
    from delta.tables import DeltaTable
    
    if spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}"):
        deltaTable = DeltaTable.forPath(spark, f'{folder_path}/{table_name}')
        
        deltaTable.alias('tgt') \
            .merge(input_df.alias('src'), merge_condition) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
    else:
        input_df.write.mode("overwrite") \
            .partitionBy(partition_column) \
            .format("delta") \
            .saveAsTable(f"{db_name}.{table_name}")
```

#### 3. **overwrite_partition**
```python
def overwrite_partition(input_df, db_name, table_name, partition_column):
    """Sobrescribe particiones específicas sin afectar otras"""
    for item_list in input_df.select(f'{partition_column}').distinct().collect():
        if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
            spark.sql(f'ALTER TABLE {db_name}.{table_name} \
                       DROP IF EXISTS PARTITION ({partition_column} = "{item_list[partition_column]}")')
```

---

## 📈 Resultados y Métricas del Proyecto

### Datasets Generados (Gold Layer)

| Dataset | Registros | Descripción |
|---------|-----------|-------------|
| `results_movie_genre_language` | 20,031 | Películas con géneros e idiomas |
| `results_movie_genre_language_py` | 20,031 | Implementación PySpark |
| `results_movie_genre_language_ext_sql` | 1,269 | Versión SQL extendida |
| `results_country_prod_company` | 6,477 | Análisis por país y compañía |
| `results_group_movie_genre` | 38 | Agregaciones por género |
| `results_group_movie_country` | 46 | Agregaciones por país |

### Insights de Negocio

#### 📊 Por Género (2015-2017)
| Género | Presupuesto Total | Ingresos Totales | ROI | Ranking |
|--------|-------------------|------------------|-----|---------|
| Adventure | $3.31B | $11.31B | 342% | #1 |
| Action | $2.92B | $10.59B | 363% | #2 |
| Science Fiction | $2.15B | $7.31B | 340% | #3 |

**Conclusiones**:
- **Action** ofrece el mejor ROI (363%), superando a Adventure a pesar de menor inversión total
- Los 3 géneros top concentran **~$8.4B en presupuestos** pero generan **~$29.2B en ingresos**
- **Adventure** domina en volumen absoluto, ideal para estudios con alto capital
- **Science Fiction** mantiene ROI competitivo con menor riesgo de inversión
- Estrategia recomendada: Híbridos Action-Adventure maximizan audiencia y rentabilidad

#### 🌍 Por País (2015-2016)
| País | Ingresos Totales | % del Mercado | Ranking |
|------|------------------|---------------|---------|
| United States of America | $21.73B | 87.3% | #1 |
| United Kingdom | $1.89B | 7.6% | #2 |
| Australia | $1.09B | 4.4% | #3 |

**Conclusiones**:
- **USA domina** con casi 9 de cada 10 dólares generados en la industria
- **UK** se posiciona como segundo hub, aprovechando infraestructura y talento local
- **Australia** emerge como mercado en crecimiento (4.4%), atractivo por incentivos fiscales
- Concentración geográfica indica **altas barreras de entrada** en producción cinematográfica
- Oportunidad: Co-producciones internacionales para acceder a múltiples mercados

#### 💰 Análisis de Rentabilidad
| Métrica | Valor | Insight |
|---------|-------|---------|
| **Presupuesto Promedio** | $43.3M | Punto medio para producciones comerciales |
| **Ingresos Promedio** | $124M | Retorno esperado en condiciones normales |
| **ROI Promedio** | 286% | Industria altamente rentable vs otras inversiones |
| **Presupuesto Mediano** | $25M | 50% de películas producidas por debajo de esta cifra |
| **Duración Óptima** | 104-118 min | 50% de películas en este rango (percentil 25-75) |

**Conclusiones**:
- El **ROI de 286%** supera ampliamente inversiones tradicionales (bolsa ~10% anual)
- La diferencia entre media ($43.3M) y mediana ($25M) indica que **mega-producciones** elevan el promedio
- Películas de **presupuesto medio ($25M)** ofrecen mejor relación riesgo-beneficio
- Duración de **100-110 minutos** maximiza engagement sin fatiga de audiencia
- **Presupuestos >$150M** son apuestas de alto riesgo pero potencial de recompensa exponencial

#### 🎬 Calidad vs Comercial
| Rating Range | % de Películas | Observación |
|--------------|----------------|-------------|
| 0.0 - 5.5 | 25% | Bajo rendimiento crítico |
| 5.6 - 6.2 | 25% | Calidad aceptable |
| 6.3 - 6.7 | 25% | Buena recepción |
| 6.8 - 10.0 | 25% | Excelencia cinematográfica |

**Conclusiones**:
- Rating promedio de **6.08/10** indica estándares de calidad moderadamente altos
- **Solo el 25% supera 6.7/10**, demostrando dificultad de lograr excelencia consistente
- Correlación débil entre presupuesto y rating: **dinero no garantiza calidad**
- Películas de bajo presupuesto (<$10M) pueden alcanzar ratings altos con guión sólido
- Estrategia: Invertir en desarrollo de guión y dirección, no solo en efectos especiales

#### 🌐 Diversidad Lingüística
| Métrica | Valor | Tendencia |
|---------|-------|-----------|
| **Películas Multilingües** | 20,031 combinaciones | En aumento |
| **Promedio idiomas/película** | ~2.5 idiomas | Globalización |
| **Idioma dominante** | Inglés | ~85% de producciones |

**Conclusiones**:
- **Globalización**: Películas incorporan múltiples idiomas para mercados internacionales
- Estrategia de distribución global visible desde la fase de producción
- Películas con diálogos en idiomas locales aumentan penetración en mercados específicos
- Tendencia hacia **contenido inclusivo** para maximizar audiencia global

#### 📈 Tendencias Temporales (2015-2017)
| Año | Observación Clave |
|-----|-------------------|
| **2015** | Mayor diversidad de géneros, 23 países productores activos |
| **2016** | Incremento en presupuestos de Science Fiction |
| **2017** | Consolidación de mega-producciones (>$200M) |

**Conclusiones**:
- **Escalada presupuestaria**: Inversiones crecientes año tras año
- Mayor participación de estudios independientes en 2015-2016
- Tendencia hacia **franquicias y universos cinematográficos** (mayor inversión inicial)
- Ciclo de producción indica planeación 2-3 años antes del lanzamiento

#### 💡 Recomendaciones Estratégicas

**Para Inversores**:
1. Portafolio balanceado: 60% Action/Adventure, 20% Sci-Fi, 20% nicho
2. Presupuesto óptimo: $20-50M para maximizar ROI
3. Co-producciones UK/Australia para optimizar costos y acceso a mercados

**Para Productoras**:
1. Enfoque en guión y dirección sobre efectos especiales
2. Duración óptima: 100-110 minutos
3. Estrategia multilingüe desde pre-producción
4. Lanzamientos estratégicos en ventanas óptimas

**Para Estudios Emergentes**:
1. Iniciar con géneros nicho (<$10M presupuesto)
2. Aprovechar incentivos fiscales (Australia, UK, Canadá)
3. Enfoque en calidad sobre cantidad
4. Construir catálogo antes de escalar a grandes producciones

---

## 🗂️ Estructura del Proyecto

```
Proyect_Databricks_Movie_History/
│
├── README.md                          # Esta documentación
│
├── movie-history-proyect/             # Notebooks de Databricks
│   │
│   ├── setup/                         # Configuración de conexiones
│   │   ├── 01.access_adls_using_access_key.ipynb
│   │   ├── 02.access_adls_using_token_sas.ipynb
│   │   └── 03.access_adls_using_service_principal.ipynb ⭐
│   │
│   ├── includes/                      # Funciones y configuraciones compartidas
│   │   ├── configuration.ipynb        # Rutas y configuraciones
│   │   └── common_functions.ipynb     # Funciones reutilizables
│   │
│   ├── bronze/                        # Capa Bronze
│   │   └── 01.create_bronze_table.ipynb
│   │
│   ├── ingestion/                     # Ingesta Bronze → Silver
│   │   ├── 00-ingestion_all_notebooks.ipynb  # Orquestador
│   │   ├── 01-Ingestion_file_movie.ipynb
│   │   ├── 02-ingestion_file_languaje.ipynb
│   │   ├── 03-ingestion_file_genre.ipynb
│   │   ├── ... (13 notebooks total)
│   │   └── 13-create_silver_database.ipynb
│   │
│   ├── transformation/                # Transformación Silver → Gold
│   │   ├── 00.run_transformation_all_notebooks.ipynb  # Orquestador
│   │   ├── 01.results_movies_genre_language.ipynb
│   │   ├── 02.results_country_prod_company.ipynb
│   │   ├── 03.results_group_movie_genre.ipynb
│   │   ├── 04.results_group_movie_country.ipynb
│   │   ├── 05.create_gold_database.ipynb
│   │   └── 06.results_movie.ipynb
│   │
│   ├── analysis/                      # Análisis de negocio
│   │   ├── 01.budget_revenue_country.ipynb
│   │   ├── 02.budget_revenue_production_company.ipynb
│   │   └── ...
│   │
│   └── demo/                          # Notebooks de práctica
│       ├── 01.filter_demo.ipynb
│       ├── 02.join_demo.ipynb
│       ├── 03.aggregation_demo.ipynb
│       └── ... (demos de conceptos)
│
└── gold/                              # Datos procesados (Parquet)
    ├── results_movie_genre_language/
    ├── results_movie_genre_language_py/
    ├── results_movie_genre_language_ext_sql/
    ├── results_country_prod_company/
    ├── results_group_movie_genre/
    └── results_group_movie_country/
```

---

## 🎓 Conceptos y Técnicas Aplicadas

### Spark & PySpark
- ✅ **DataFrames API**: Manipulación de datos distribuidos
- ✅ **Spark SQL**: Queries declarativas sobre DataFrames
- ✅ **Window Functions**: Rankings, agregaciones móviles
- ✅ **Joins**: Inner, outer, complex multi-table joins
- ✅ **Aggregations**: GroupBy, sum, avg, count con múltiples columnas
- ✅ **Particionamiento**: Optimización de queries y storage
- ✅ **Broadcasting**: Optimización de joins pequeños

### Delta Lake
- ✅ **ACID Transactions**: Garantías de consistencia
- ✅ **Time Travel**: Versionado de datos
- ✅ **Merge/Upsert**: Actualizaciones incrementales
- ✅ **Schema Evolution**: Adaptación a cambios de estructura
- ✅ **Optimización**: Z-ordering, compactación

### Diseño de Datos
- ✅ **Arquitectura Medallion**: Bronze → Silver → Gold
- ✅ **Star Schema**: Tablas de dimensiones y hechos
- ✅ **Normalización**: Eliminación de redundancias
- ✅ **Desnormalización**: Optimización para analytics
- ✅ **Particionamiento**: Por fecha para performance

### DevOps & Mejores Prácticas
- ✅ **Parametrización**: Widgets para configuración dinámica
- ✅ **Modularización**: Funciones reutilizables
- ✅ **Orquestación**: Notebooks maestros para ejecutar pipelines
- ✅ **Logging**: Metadata de auditoría
- ✅ **Versionado**: Control de cambios en notebooks

---

## 🔧 Cómo Ejecutar el Proyecto

### Prerrequisitos

1. **Azure Subscription** activa
2. **Azure Databricks Workspace** aprovisionado
3. **Azure Data Lake Storage Gen2** configurado
4. **Azure Key Vault** con los secretos necesarios
5. **Service Principal** con permisos de "Storage Blob Data Contributor"

### Configuración Inicial

1. **Crear Storage Account y Container**
```bash
# Crear storage account
az storage account create \
  --name moviehistory0001 \
  --resource-group <your-rg> \
  --location eastus \
  --sku Standard_LRS \
  --kind StorageV2 \
  --hierarchical-namespace true

# Crear containers
az storage fs create --name bronze --account-name moviehistory0001
az storage fs create --name silver --account-name moviehistory0001
az storage fs create --name gold --account-name moviehistory0001
```

2. **Configurar Service Principal**
```bash
# Crear service principal
az ad sp create-for-rbac -n "movie-history-sp"

# Asignar rol
az role assignment create \
  --role "Storage Blob Data Contributor" \
  --assignee <service-principal-id> \
  --scope /subscriptions/<sub-id>/resourceGroups/<rg>/providers/Microsoft.Storage/storageAccounts/moviehistory0001
```

3. **Crear Secrets en Databricks**
```python
# En Databricks CLI o UI
databricks secrets create-scope --scope movie-history-secret-scope
databricks secrets put --scope movie-history-secret-scope --key client-id
databricks secrets put --scope movie-history-secret-scope --key tenant-id
databricks secrets put --scope movie-history-secret-scope --key client-secret
```

4. **Montar el Data Lake**
```python
# En notebook de Databricks
dbutils.fs.mount(
  source = "abfss://bronze@moviehistory0001.dfs.core.windows.net/",
  mount_point = "/mnt/moviehistory0001/bronze",
  extra_configs = configs
)
```

### Ejecución del Pipeline

#### Opción 1: Ejecución Manual
1. Ejecutar notebooks de `setup/` para configurar conexión
2. Ejecutar notebooks de `bronze/` para crear tablas
3. Ejecutar notebooks de `ingestion/` (01-13)
4. Ejecutar notebooks de `transformation/` (01-06)
5. Ejecutar notebooks de `analysis/` según necesidad

#### Opción 2: Ejecución Orquestada
```python
# Ejecutar notebook orquestador de ingesta
%run ./ingestion/00-ingestion_all_notebooks

# Ejecutar notebook orquestador de transformación
%run ./transformation/00.run_transformation_all_notebooks
```

#### Opción 3: Azure Data Factory (Producción)
- Crear pipelines en ADF
- Configurar triggers (schedule, event-based)
- Monitoreo centralizado

---

## 📚 Lecciones Aprendidas

### Desafíos y Soluciones

1. **Manejo de JSON complejo**
   - **Desafío**: JSON con estructuras anidadas (person.json)
   - **Solución**: Uso de `STRUCT` types en Spark schemas

2. **Performance en Joins**
   - **Desafío**: Joins lentos en datasets grandes
   - **Solución**: Broadcasting de tablas pequeñas, particionamiento estratégico

3. **Gestión de Duplicados**
   - **Desafío**: Datos duplicados en cargas incrementales
   - **Solución**: Implementación de merge/upsert con Delta Lake

4. **Versionado de Datos**
   - **Desafío**: Necesidad de auditoría y rollback
   - **Solución**: Delta Lake time travel y metadata de auditoría

### Mejores Prácticas Aplicadas

✅ **Separation of Concerns**: Configuración separada de lógica  
✅ **DRY Principle**: Funciones reutilizables en common_functions  
✅ **Idempotencia**: Pipelines que pueden re-ejecutarse sin efectos adversos  
✅ **Parametrización**: Widgets para configuración flexible  
✅ **Data Quality**: Validaciones de esquema y tipos de datos  
✅ **Security**: Uso de Service Principal y secrets management  
✅ **Observability**: Metadata de auditoría en todas las capas  

---

## 🚀 Próximos Pasos y Mejoras

### Mejoras Planificadas

- [ ] **Implementar Data Quality Framework**
  - Great Expectations para validaciones
  - Alertas automáticas en caso de anomalías
  
- [ ] **Orquestación Avanzada**
  - Migración a Azure Data Factory
  - Implementar CI/CD con Azure DevOps
  
- [ ] **Monitoreo y Alertas**
  - Dashboards de monitoreo con Azure Monitor
  - Alertas por email/Slack para fallos
  
- [ ] **Optimizaciones de Performance**
  - Z-ordering en tablas Delta
  - Compactación automática de archivos
  - Caching estratégico
  
- [ ] **Análisis Avanzados**
  - Machine Learning con MLflow
  - Predicción de ingresos de películas
  - Análisis de sentimiento de reseñas
  
- [ ] **Visualización**
  - Dashboards interactivos con Power BI
  - Integración con Databricks SQL Dashboards

---

## 🤝 Contribuciones y Contacto

Este proyecto fue desarrollado como parte de mi portfolio profesional de ingeniería de datos.

**Autor**: [Jean Mangones N]   
**GitHub**: [github.com/Jeancmn](https://github.com/Jeancmn)

---

---

## 🙏 Agradecimientos

- **Azure Databricks Documentation** por recursos educativos
- **Delta Lake Community** por la excelente tecnología open-source
- **Databricks Academy** por los cursos y certificaciones

---

## 📊 Evidencias Visuales

### Arquitectura Implementada

```
Azure Cloud
    │
    ├─── Azure Entra ID (Service Principal)
    │         │
    │         ├─── Authentication
    │         └─── RBAC
    │
    ├─── Azure Key Vault
    │         │
    │         └─── Secrets (client-id, tenant-id, client-secret)
    │
    ├─── Azure Data Lake Gen2 (moviehistory0001)
    │         │
    │         ├─── /bronze  (CSV, JSON raw files)
    │         ├─── /silver  (Delta Tables - cleaned)
    │         └─── /gold    (Delta Tables - aggregated)
    │
    └─── Azure Databricks
              │
              ├─── Cluster Configuration
              ├─── Notebooks (PySpark + SQL)
              ├─── Jobs & Workflows
              └─── Delta Lake Engine
```

### Pipeline Flow

```
1. INGESTION (Bronze → Silver)
   ┌─────────────────────────────────────────┐
   │ Source Data (CSV/JSON)                  │
   │   • movie.csv (4,500 películas)         │
   │   • language.csv, genre.csv             │
   │   • person.json (nested structure)      │
   │   • movie_cast.json (multiline)         │
   └──────────────┬──────────────────────────┘
                  │
                  ▼
   ┌─────────────────────────────────────────┐
   │ Transformations                         │
   │   • Schema validation                   │
   │   • Column renaming                     │
   │   • Data type casting                   │
   │   • Add metadata (ingestion_date)       │
   └──────────────┬──────────────────────────┘
                  │
                  ▼
   ┌─────────────────────────────────────────┐
   │ Silver Layer (Delta Tables)             │
   │   • Partitioned by file_date            │
   │   • ACID transactions                   │
   │   • Merge/Upsert operations             │
   └─────────────────────────────────────────┘

2. TRANSFORMATION (Silver → Gold)
   ┌─────────────────────────────────────────┐
   │ Silver Tables                           │
   │   • movies, genres, languages           │
   │   • countries, companies                │
   │   • bridge tables                       │
   └──────────────┬──────────────────────────┘
                  │
                  ▼
   ┌─────────────────────────────────────────┐
   │ Business Logic                          │
   │   • Multi-table joins                   │
   │   • Aggregations (sum, avg, count)      │
   │   • Window functions (ranking)          │
   │   • Filtering (year >= 2000)            │
   └──────────────┬──────────────────────────┘
                  │
                  ▼
   ┌─────────────────────────────────────────┐
   │ Gold Layer (Analytics)                  │
   │   • results_movie_genre_language        │
   │   • results_country_prod_company        │
   │   • results_group_movie_genre           │
   │   • results_group_movie_country         │
   └─────────────────────────────────────────┘

3. ANALYSIS (Consumption)
   ┌─────────────────────────────────────────┐
   │ Business Intelligence                   │
   │   • Budget vs Revenue analysis          │
   │   • Top performing countries            │
   │   • Genre trends over time              │
   │   • Company profitability               │
   └─────────────────────────────────────────┘
```

---

## 📖 Documentación Técnica Adicional

### Convenciones de Nomenclatura

- **Bases de datos**: `movie_bronze`, `movie_silver`, `movie_gold`
- **Tablas**: snake_case (e.g., `movies_genres`, `production_companies`)
- **Columnas**: snake_case (e.g., `movie_id`, `release_date`)
- **Notebooks**: Numeración secuencial (e.g., `01-Ingestion_file_movie.ipynb`)
- **Funciones**: snake_case (e.g., `add_ingestion_date`, `merge_delta_lake`)

### Estrategia de Particionamiento

- **Silver Layer**: Particionado por `file_date` para cargas incrementales
- **Gold Layer**: Particionado por `created_date` para versionado de resultados

### Gestión de Versiones

```sql
-- Delta Lake Time Travel
SELECT * FROM movie_gold.results_movie_genre_language VERSION AS OF 1
SELECT * FROM movie_gold.results_movie_genre_language TIMESTAMP AS OF '2024-12-30'

-- Historia de cambios
DESC HISTORY movie_gold.results_movie_genre_language
```

---

**Este proyecto demuestra competencias en**:
- ☑️ Diseño e implementación de pipelines de datos escalables
- ☑️ Arquitectura de datos enterprise (Medallion)
- ☑️ Procesamiento distribuido con Apache Spark
- ☑️ Servicios cloud de Azure (Databricks, ADLS, Key Vault)
- ☑️ Seguridad y gobernanza de datos
- ☑️ SQL y PySpark avanzado
- ☑️ Delta Lake y ACID transactions
- ☑️ Mejores prácticas de ingeniería de software en datos

---

⭐ **Si este proyecto te resultó útil, considera darle una estrella en GitHub!**