import os
import json
import sys
import io
from collections import OrderedDict

from osgeo import gdal
from gdalconst import GA_ReadOnly

ENCODING = "latin1"
os.environ['GDAL_DATA'] = "C:\Program Files (x86)\GDAL\gdal-data"
gdal.UseExceptions()


# [path] land_shallow_topo_2048.tif
# [ bands] =  3
# [path] nat_color.tif
# [ bands] =  3


def open_fw(file_name, encoding=ENCODING, encode=True):
    """Open file for writing respecting Python version and OS differences.

    Sets newline to Linux line endings on Python 3
    When encode=False does not set encoding on nix and Python 3 to keep as bytes
    """
    if sys.version_info >= (3, 0, 0):
        if encode:
            file_obj = io.open(file_name, 'w', newline='', encoding=encoding)
        else:
            file_obj = io.open(file_name, 'w', newline='')
    else:
        file_obj = io.open(file_name, 'wb')
    return file_obj


class Datasets(object):
    simple_formats = ['.gif', '.img', '.bil', '.jpg', '.tif', '.tiff', '.hdf', '.l1b']
    multi_formats = ["hdf"]

    def __init__(self):
        self.data_sets = []
        self.firm = []

    def readsource(self, pathx):
        """Read source folder or file and return list of Raster files
        """
        if os.path.isfile(os.path.normpath(pathx)):
            self.firm = [os.path.normpath(pathx)]
        else:
            self.firm = [os.path.normpath(os.path.join(root, name))
                         for root, dirs, files in
                         os.walk(pathx, topdown=False) for name in files]

    def getbands(self, pathx):
        """Get all the bands from the source files"""
        self.readsource(pathx)

        for i in self.firm:
            if os.path.isfile(i) and os.path.splitext(os.path.normpath(i))[1] in self.simple_formats:
                fomartx = os.path.splitext(os.path.normpath(i))[1][1:]
                pack = OrderedDict()
                file_name = os.path.basename(i)
                base = os.path.splitext(file_name)[0]
                pack["name"] = base

                # try:
                if True:
                    if fomartx not in self.multi_formats:
                        mysubdataset_name = i
                        # src_ds = gdal.Open(mysubdataset_name, GA_ReadOnly)
                        #
                        # # required
                        # base_str = os.path.basename(src_ds.GetDescription())
                        # # pack["name"]
                        # pack["url"] = "link to where the data is stored"
                        #
                        # # when applicable
                        # pack["title"] = "--A titile for the dataset"
                        # pack["driver"] = src_ds.GetDriver().ShortName
                        # pack["description"] = "--A good description for the dataset"
                        # pack["license"] = "--A license"
                        # pack["keywords"] = ["test", "data science", "spatial-data"]
                        # pack["citation"] = "--citation for the dataset"
                        # pack["version"] = "--The version of the dataset"
                        # pack["homepage"] = "--The home page of the data"
                        # pack["colums"] = src_ds.RasterXSize
                        # pack["rows"] = src_ds.RasterYSize
                        # pack["band_count"] = src_ds.RasterCount
                        # pack["datum"] = "--Coordinate Reference System"
                        # pack["projection"] = src_ds.GetProjection()
                        # pack["file_size"] = "--size of file on disk"
                        # pack["group_count"] = "--Number of groups in the dataset if applicable"
                        # pack["dataset_count"] = "--The number of individual datasets"
                        # pack["transform"] = OrderedDict(
                        #     zip(["xOrigin", "pixelWidth", "rotation_2", "yOrigin", "rotation_4", "pixelHeight"],
                        #         src_ds.GetGeoTransform()))
                        # pack["resources"] = []
                        #
                        # # print("[ Metadata ] = ", src_ds.GetMetadata())     # dont get metadata at band level
                        # for band_num in range(1, src_ds.RasterCount + 1):
                        #     bands = OrderedDict()
                        #     srcband = src_ds.GetRasterBand(band_num)
                        #
                        #     bands['band_name'] = pack["name"] + "_" + str(band_num)
                        #     bands["no_data_value"] = srcband.GetNoDataValue()
                        #     bands["min"] = srcband.GetMinimum()
                        #     bands["max"] = srcband.GetMaximum()
                        #     bands["scale"] = srcband.GetScale()
                        #     bands["color_table"] = None if not srcband.GetRasterColorTable() else True
                        #
                        #     bands["statistics"] = OrderedDict(
                        #         zip(["minimum", "maximum", "mean", "stddev"], srcband.GetStatistics(True, False)))
                        #     pack["resources"].append(bands)
                        #
                        # file_path_source = pack["name"] + ".json"
                        # with open_fw(file_path_source) as output_spec_datapack:
                        #     json_str = json.dumps(pack, output_spec_datapack, sort_keys=False, indent=4,
                        #                           separators=(',', ': '))
                        #     output_spec_datapack.write(json_str + '\n')
                        #
                        #     output_spec_datapack.close()

                    if fomartx in self.multi_formats:
                        files_sc = gdal.Open(i, GA_ReadOnly)
                        datasets = files_sc.GetSubDatasets()
                        # when applicable
                        pack["url"] = "link to where the data is stored"
                        pack["description"] = "--A good description for the dataset"
                        pack["license"] = "--A license"
                        pack["keywords"] = ["test", "data science", "spatial-data"]
                        pack["citation"] = "--citation for the dataset"
                        pack["version"] = "--The version of the dataset"
                        pack["homepage"] = "--The home page of the data"
                        pack["resources"] = []


                        for i_subs in datasets:
                            dataset_name = str(i_subs[0]).split(".{}\":".format(fomartx))[1]
                            mysubdataset_name = i_subs[0]
                            # print(mysubdataset_name.split(":")[-1], "-----llll", i[0])
                            src_ds = gdal.Open(mysubdataset_name, gdal.GA_ReadOnly)

                            print(dataset_name)

                            pack["projection"] = src_ds.GetProjection()
                            pack["driver"] = src_ds.GetDriver().ShortName
                            pack["transform"] = OrderedDict(zip(["xOrigin", "pixelWidth", "rotation_2", "yOrigin", "rotation_4", "pixelHeight"],  src_ds.GetGeoTransform()))
                            pack["colums"] = src_ds.RasterXSize
                            pack["rows"] = src_ds.RasterYSize
                            pack["band_count"] = src_ds.RasterCount



                            for band_num in range(1, src_ds.RasterCount + 1):
                                srcband = src_ds.GetRasterBand(band_num)
                                bands = OrderedDict()
                                bands['band_name'] = dataset_name.split(":")[-1]
                                bands["description"] = srcband.GetDescription()
                                bands["no_data_value"] = srcband.GetNoDataValue()
                                bands["min"] = srcband.GetMinimum()
                                bands["max"] = srcband.GetMaximum()
                                bands["scale"] = srcband.GetScale()
                                bands["color_table"] = None if not srcband.GetRasterColorTable() else True

                                bands["statistics"] = OrderedDict(
                                    zip(["minimum", "maximum", "mean", "stddev"], srcband.GetStatistics(True, False)))
                                pack["resources"].append(bands)

                                file_path_source = pack["name"].replace(".","_") + ".json"

                                with open_fw(file_path_source) as output_spec_datapack:
                                    json_str = json.dumps(pack, output_spec_datapack, sort_keys=False, indent=4,
                                                          separators=(',', ': '))
                                    output_spec_datapack.write(json_str + '\n')

                                    output_spec_datapack.close()
                                exit()

file_path = r"C:\Users\Henry\PycharmProjects\gdalandgis\gdaltest\data\hdffiles"
file_path = r"C:\Users\Henry\PycharmProjects\gdalandgis\gdaltest\data\bio"
os.chdir(r"C:\Users\Henry\Documents\GitHub\weaver\raster_packs")
v = Datasets()
print(os.path.abspath(os.path.curdir))
v.getbands(file_path)

# print(v.firm)
