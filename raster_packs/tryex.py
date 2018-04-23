import os
import json
import sys
import io
from collections import OrderedDict

from osgeo import gdal
from gdalconst import GA_ReadOnly

ENCODING = "latin1"
os.environ['GDAL_DATA'] = r"C:\Users\Henry\PycharmProjects\gdalandgis\gdaltest\data\bio"
gdal.UseExceptions()


gdal.GetDriverByName('EHdr').Register()
i = r"C:\Users\Henry\PycharmProjects\gdalandgis\gdaltest\data\bio\bio1.bil"
t = r"C:\Users\Henry\PycharmProjects\gdalandgis\gdaltest\data\bio\bio1.hdr"

hdf_ds = gdal.Open(i, gdal.gdalconst.GA_ReadOnly)

img = gdal.Open(i, gdal.gdalconst.GA_ReadOnly)
band = img.GetRasterBand(1)
geotransform = img.GetGeoTransform()
print(geotransform)

geotransform = band.GetMetadata()
print(geotransform)
print(dir(img))


geotransform = img.GetProjection()
print(geotransform)
# # i = r"C:\Users\Henry\PycharmProjects\gdalandgis\gdaltest\data\bio\land_shallow_topo_2048.tif"
# # files_sc = gdal.Open(i, GA_ReadOnly)
# # print(files_sc)
#
# gdal.GetDriverByName('EHdr').Register()
# i = r"C:\Users\Henry\PycharmProjects\gdalandgis\gdaltest\data\bio\MOD11A1.A2012193.h00v08.005.2012196013548.hdf"
# # files_sc = gdal.Open(i, GA_ReadOnly)


