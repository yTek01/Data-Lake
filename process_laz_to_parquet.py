import laspy
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
import numpy as np
import sys
import traceback
#import pylas
import laspy
import os


def process_laz_data(data_dir="/home/gitpod/whitebox_workflows/Kitchener_lidar/Kitchener_lidar.las"):
    las = laspy.read(data_dir)

    #infile = laspy.file.File('/home/gitpod/whitebox_workflows/Kitchener_lidar/new_lidar.laz', mode='r')
    coords = np.vstack((las.x, las.y, las.z)).transpose()
    print("Done with the Coords", coords)

    gdf = gpd.GeoDataFrame(columns=['geometry'], geometry=[Point(xyz) for xyz in coords])
    gdf.crs = {'init': 'epsg:4326'}

    gdf.to_file('output_filename.shp')

    gdf['geometry'] = gdf['geometry'].apply(lambda x: x.wkt)

    df = pd.DataFrame(gdf)
    df.to_parquet('lidar_data_in_parque.parquet')

def convert_laz_to_las(in_laz, out_las):
    las = laspy.read(in_laz)
    las = laspy.convert(las)
    las.write(out_las) 

def laz_to_las(in_dir):
    try:
        print('Running LAZ_to_LAS.py')
        
        for (dirpath, dirnames, filenames) in os.walk(in_dir):
            for inFile in filenames:
                if inFile.endswith('.laz'):	
                    in_laz = os.path.join(dirpath,inFile)
                    
                    out_las = in_laz.replace('laz', 'las') 
                    print('working on file: ',out_las)
                    convert_laz_to_las(in_laz, out_las)

        print('Finished without errors - LAZ_to_LAS.py')
    except:
        tb = sys.exc_info()[2]
        tbinfo = traceback.format_tb(tb)[0]
        print('Error in read_xmp.py')
        print ("PYTHON ERRORS:\nTraceback info:\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1]))   

if __name__ == "__main__":
    #laz_to_las("/home/gitpod/whitebox_workflows/Kitchener_lidar/")
    #process_laz_data()