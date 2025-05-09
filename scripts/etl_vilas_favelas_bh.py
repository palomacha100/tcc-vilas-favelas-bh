import sys
import pandas as pd
from shapely import wkt
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.dynamicframe import DynamicFrame
import geopandas as gpd

# Receives job parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'SOURCE_BUCKET', 'SOURCE_FILE', 'DEST_BUCKET', 'DEST_FILE'])

# Creates the Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Read CSV file from S3
source_path = f"s3://{args['SOURCE_BUCKET']}/{args['SOURCE_FILE']}"
df_vilas_favelas = pd.read_csv(source_path)

# Convert the 'geometry' column to geometry format using WKT (Well-Known Text)
df_vilas_favelas["geometry"] = df_vilas_favelas["geometria"].apply(wkt.loads)

# Convert Pandas DataFrame to GeoDataFrame for geometry manipulation
gdf_vilas_favelas = gpd.GeoDataFrame(df_vilas_favelas, geometry="geometry", crs="EPSG:31983")

# Transforms coordinates to the appropriate CRS (EPSG:4326 to WGS84)
gdf_vilas_favelas = gdf_vilas_favelas.to_crs(epsg=4326)

# Change the name of the 'locale' column to ensure it is in the correct format
gdf_vilas_favelas['localidade'] = gdf_vilas_favelas['localidade'].astype(str).str.strip()

# Convert GeoDataFrame to Pandas DataFrame
df_vilas_favelas_clean = pd.DataFrame(gdf_vilas_favelas)

# Convert this DataFrame to a DynamicFrame (Glue's format for processing)
dynamic_frame = DynamicFrame.from_pandas(df_vilas_favelas_clean, glueContext)

# Specifies the destination path to save the transformed data as Parquet
dest_path = f"s3://{args['DEST_BUCKET']}/{args['DEST_FILE']}"

# Records the transformed data in Parquet format
glueContext.write_dynamic_frame.from_options(dynamic_frame, connection_type="s3", connection_options={"path": dest_path}, format="parquet")

# Record the job completion
job.commit()



