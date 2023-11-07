import laspy
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
import numpy as np
import sys
import traceback
#import pylas
import numpy as np
import pylas
import pyvista as pv
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


def convert_laz_to_las():
    las = pylas.read('data_lakehouse/ot_TNF_003360.laz')    
    las = pylas.convert(las)    
    las.write('data_lakehouse/ot_TNF_003360.las')


def get_laz_img_attr(filename='filename.laz'):
    with laspy.open(filename) as f:
        header_metadata = f.header
        attributes = header_metadata.point_format.dimensions
        print(attributes)

def read_in_laz_data(filename='your_file.laz'):
    with laspy.open(filename) as f:
        las = f.read()
    df = pd.DataFrame({
        'x': las.x,
        'y': las.y,
        'z': las.z,
        'intensity': las.intensity,  
        'gps_time': las.gps_time,    
        'user_data': las.user_data
    })
    return df, las


def plot_data_points(point_data, intensity):
    cloud = pv.PolyData(point_data)
    cloud['intensity'] = intensity  
    plotter = pv.Plotter()
    plotter.add_points(cloud, color='white')
    plotter.show()


def stack_data_points(las):
    point_data = np.stack([las.X, las.Y, las.Z], axis=0).transpose((1, 0))
    return point_data, las.intensity

def read_laz_using_pylas(filename='your_file.laz'):
    with pylas.open(filename) as fh:
        print('Points from Header:', fh.header.point_count)
        las = fh.read()
        print(las)
        print('Points from data:', len(las.points))
        ground_pts = las.classification == 2
        bins, counts = np.unique(las.return_number[ground_pts], return_counts=True)
        print('Ground Point Return Number distribution:')
        for r,c in zip(bins,counts):
            print('    {}:{}'.format(r,c))


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
    #get_laz_img_attr(filename='ot_TNF_003360.laz')
    df, las = read_in_laz_data(filename='ot_TNF_003360.laz')
    print(df)
    point_data, intensity = stack_data_points(las)
    plot_data_points(point_data, intensity)
    #read_laz_using_pylas('data_lakehouse/ot_TNF_003360.laz')
    #convert_laz_to_las()