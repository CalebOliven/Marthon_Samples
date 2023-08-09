# Collaborative Effort with ESRI POD and Dynamap Team
# Contributing authors: ###########,##############,##########,coliven@contractor.usgs.gov, ###########, ###############, ################


"""This python toolbox contains tools for
creating a new map for the Fixed 25K Product"""
import arcpy
import arcpyproduction
import os
import sys
import json
import shutil
import traceback
import datetime
import subprocess
import re
import os.path
import zipfile
import gc
import time

## USGS Integration
import boto3
from botocore.exceptions import ClientError
import psycopg2
import zipfile



# The current architecture requires that the POD product files be located in the
# 'arcgisserver' folder. If this needs to be changed, the following four lines
# of code must be modified to point to the correct location
SCRIPTPATH = os.path.abspath(__file__)
SCRIPTSDIRECTORY = os.path.abspath(os.path.join(SCRIPTPATH, "Utilities"))
PARENTDIRECTORY = os.path.splitdrive(SCRIPTPATH)
UNCPARENTDIRECTORY = os.path.splitunc(SCRIPTPATH)

# This syntax is required for UNC paths
if PARENTDIRECTORY[0] == "":
    SCRIPTSDIRECTORY = os.path.join(UNCPARENTDIRECTORY[0], r'be_dynamap\Utilities')
    arcpy.AddMessage("Using UNC paths: " + SCRIPTSDIRECTORY)

# This syntax is required for local paths
else:
    SCRIPTSDIRECTORY = os.path.join(PARENTDIRECTORY[0], r'\arcgisserver\be_dynamap\Utilities')
    arcpy.AddMessage("Using local paths: " + SCRIPTSDIRECTORY)

sys.path.append(SCRIPTSDIRECTORY)
import USGS_POD_Utilities
import ContourGeneration
import Config24K
import DEMHydroEnforcement
import XMLEditor

del SCRIPTPATH, PARENTDIRECTORY, SCRIPTSDIRECTORY, UNCPARENTDIRECTORY

class Toolbox(object):
    """Toolbox classes, ArcMap needs this class."""
    def __init__(self):
        """Define the toolbox (the name of the toolbox is the name of the .pyt file)."""
        self.label = "USGS Tools"
        self.alias = "usgsTools"
        # List of tool classes associated with this toolbox
        self.tools = [MapGeneratorAutoLabeling]

class MapGeneratorAutoLabeling(object):
    """ Class that contains the code to generate a new map based off the input aoi"""

    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "Map Generation JSON"
        self.description = "Python Script used to create the a new map at a 1:24000 scale"
        self.canRunInBackground = False

        #Path to the AGS Output Directory
        self.outputdirectory = Config24K.output_directory
        # Path to MCS_POD's Product Location
        self.shared_prod_path = Config24K.shared_products_path

    def getParameterInfo(self):
        """Define parameter definitions"""
        product_as_json = arcpy.Parameter(name="product_as_json",
                                          displayName="Product As JSON",
                                          direction="Input",
                                          datatype="GPString",
                                          parameterType="Required")

        output_file = arcpy.Parameter(name="output_file",
                                      displayName="Output File",
                                      direction="Output",
                                      datatype="GPString",
                                      parameterType="Derived")

        product_as_json.value ='{"Job_AOI_Name": "24K Topographic Map", "SmoothnessLevel": "Medium", "cell_id": 18751, "contourSmoothingScale": 25000, "customName": "", "email": "test@contractor.usgs.gov", "exportOption": "Export", "exporter": "Production PDF", "geometry": {"rings": [[[-11897271.735805145, 4650302.707323042], [-11911186.675235491, 4650302.706131895], [-11911186.676753279, 4668098.378398429], [-11897271.737320825, 4668098.379598828], [-11897271.735805145, 4650302.707323042]]], "spatialReference": {"latestWkid": 3857, "wkid": 102100}}, "layersToRemove": [], "makeMapScript": "Autolabeling_MapGenerator.pyt", "mapSheetName": "Gunnison", "primary_state": "Colorado", "productName": "24K Topographic Map", "toolName": "MapGeneratorAutoLabeling", "process_id": 17383, "map_id": "#######-####-####-####-###########"}'
        
        params = [product_as_json, output_file]
        return params

    def isLicensed(self):
        """Set whether tool is licensed to execute."""
        if arcpy.CheckExtension("foundation") == "Available":
            return True
        return False
    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""
        return

    def updateMessages(self, parameters):
        """Modify the messages created by internal validation for each tool
        parameter.  This method is called after internal validation."""
        return

    def updateLayoutElements(self, layout_element_list, map_name, state_name, mapsheetname, cell_id, plss_layer, date, utm_text, counties_list, state_list, state_fipscode, state_abr, us_states_data_matrix, overlapping_quad_id, countour_interval, contour_smoothness, logfile):
        """code to update the surround elements"""

        try:

            # Updates layout elements for final map
            data_service_db = Config24K.vector_db_connection

            countour_interval_staus = 1

            # Determines the <MAP_TITLE_STATE_NAME_UPPER_RIGHT> Text
            map_title_state_text_upper_right = ""
            if cell_id == "":
                map_title_state_text_upper_right = "Custom Extent"
            else:
                if len(counties_list) == 1 and len(state_list) == 1:
                    map_title_state_text_upper_right = state_list[0].upper() + " - " + counties_list[0].upper()
                elif len(counties_list) > 1 and len(state_list) == 1:
                    map_title_state_text_upper_right = state_list[0].upper()
                elif len(state_list) >= 2:
                    s_list = state_abr.split(",")
                    for abr in s_list:
                        print (abr)
                        for state in us_states_data_matrix:
                            if abr.strip(" ") == state[0]:
                                map_title_state_text_upper_right = map_title_state_text_upper_right + state[2].upper() + " - "
                    map_title_state_text_upper_right = map_title_state_text_upper_right[:-3]

            # Updates the Surround Elements Values
            for element in layout_element_list:
                if element.name == "MAP_TITLE_UPPER_RIGHT":
                    element.text = element.text.replace('<MAP TITLE>', map_name.upper())
                elif element.name == "MAP_TITLE_STATE_NAME_UPPER_RIGHT":
                    element.text = element.text.replace('<STATE>', map_title_state_text_upper_right)
                elif element.name == "MAP_TITLE_LOWER_RIGHT":
                    element.text = element.text.replace('<MAP TITLE>', map_name.upper())
                    element.text = element.text.replace('<STATE ABR>', " " + state_abr.upper())
                elif element.name == "MAP_TITLE_YEAR_LOWER_RIGHT":
                    element.text = element.text.replace('<YEAR>', str(date.today().year))

                elif element.name == "CONTOUR_INTERVAL":
                    # Contour Text for Hawaii
                    if (state_name.upper() == "HAWAII" or state_name.upper() == "PUERTO RICO" or state_name.upper() == "VIRGIN ISLANDS") and countour_interval_staus == 1:
                        element.text = element.text.replace('<CONTOUR_INTERVAL_TEXT>', "CONTOUR INTERVAL " + str(int(countour_interval)) + " FEET\n DATUM IS LOCAL MEAN SEA LEVEL")
                    elif (state_name.upper() == "HAWAII" or state_name.upper() == "PUERTO RICO" or state_name.upper() == "VIRGIN ISLANDS") and countour_interval_staus != 1:
                        element.text = element.text.replace('<CONTOUR_INTERVAL_TEXT>', "CONTOURS NOT PRESENT BECAUSE AVAILABLE ELEVATION\nDATA DO NOT MEET ACCURACY REQUIREMENTS")
                    # Add Contour Text here for any Pacific Islands



                    # Contour Text for both CONUS and Alaska
                    elif countour_interval_staus == 1:
                        element.text = element.text.replace('<CONTOUR_INTERVAL_TEXT>', "CONTOUR INTERVAL " + str(int(countour_interval)) + " FEET\nNORTH AMERICAN VERTICAL DATUM OF 1988")
                    elif countour_interval_staus != 1:
                        element.text = element.text.replace('<CONTOUR_INTERVAL_TEXT>', "CONTOURS NOT PRESENT BECAUSE AVAILABLE ELEVATION\nDATA DO NOT MEET ACCURACY REQUIREMENTS")
                elif element.name == "CONTOUR_SMOOTHNESS":
                    element.text = element.text.replace('<CONTOUR_SMOOTHNESS_TEXT>', "CONTOUR SMOOTHNESS = " + str(contour_smoothness) )

                elif element.name == "COORDINATE_SYSTEM":
                    element.text = element.text.replace('<UTM_ZONE>', utm_text)

            del layout_element_list
            USGS_POD_Utilities.logfilewrite(logfile, "Updated the Layout Surround Elements...")
            return
        except arcpy.ExecuteError:
            arcpy.AddError(arcpy.GetMessages(2))
            messages = 'The Map Export Process Failed. Error Message:  {}'.format(arcpy.GetMessages(2))
            USGS_POD_Utilities.logfilewrite(logfile, messages)
            raise arcpy.ExecuteError(messages)
            

        except Exception as ex:
            arcpy.AddError(ex.message)
            tb = sys.exc_info()[2]
            tbinfo = traceback.format_tb(tb)[0]
            arcpy.AddError("Traceback info:\n" + tbinfo)
            messages = 'The Map Export Process Failed. Error Message:  {}'.format(ex)
            USGS_POD_Utilities.logfilewrite(logfile, messages)
            raise arcpy.ExecuteError(messages)       
            
        finally:
            gc.collect()
            logfile.flush()
            del logfile 

    def execute(self, parameters, messages):
        """The source code of the tool."""
        arcpy.AddMessage("Executing Map Generation.")
        
        date = datetime.datetime.now()
        yearStamp = date.strftime("%Y")
        dateStamp = date.strftime("%Y%m%d")
        timeStamp = date.strftime("%H%M%S%f")   
        
        
        finalURL = None

        try:
            product_json = parameters[0].value
            product = json.loads(product_json)
            product = USGS_POD_Utilities.DictToObject(product)
            arcpy.AddMessage("JSON String in: {}.".format(product_json))
            
            # temporary code to set the contour smoothing text value.  Remove this value when the Web App is sending this value.
            if "SmoothnessLevel" not in product.keys():
                if product.contourSmoothingScale == 5000:
                    product.update({u'SmoothnessLevel': "low"})
                elif product.contourSmoothingScale == 25000:
                    product.update({u'SmoothnessLevel': "medium"})
                elif product.contourSmoothingScale == 100000:
                    product.update({u'SmoothnessLevel': "high"})   
                arcpy.AddMessage("Smoothness Level is {}.".format(product.SmoothnessLevel))       
                arcpy.AddMessage("Smoothness Scale is is {}.".format(product.contourSmoothingScale))        

            # Gets the Map Name
            # Default is the Custom Name
            # uses the Map Sheet Name if the Custom Name is blank or if there are invalid characters
            map_name = product.customName.strip()
            if USGS_POD_Utilities.customNameIsValid(map_name) == False or map_name == "":
                map_name = product.mapSheetName  
            arcpy.AddMessage("Map Name is {}.".format(map_name))
            
            # PSE - do we need to mess with this whole chunk if we just reassign output_location down on line 620? - leave it in for now, 
            # is superfluous now but might be re-added
            if "batchprocessing" in product.keys():
                if product.batchprocessing == True:            
                    output_location = os.path.join(self.outputdirectory, "BatchExport")

                else:
                    output_location = self.outputdirectory
            else:
                output_location = self.outputdirectory
                
            arcpy.AddMessage("The output location is {}.".format(output_location))
                
            originalMapName = map_name
            map_name = ''.join(e for e in map_name if e.isalnum())
                
            outputFolderName = map_name
            newFolderName = "_ags_{}_{}_{}".format(outputFolderName, dateStamp, timeStamp)
            newFolderName = newFolderName.replace(" ", "_")
            newDirectory = os.path.join(output_location, newFolderName)
            if arcpy.Exists(newDirectory) == True:
                arcpy.AddError("The output directory already exsits: {}.".format(newDirectory))
                raise arcpy.ExecuteError()
                
                
            os.mkdir(newDirectory)
            output_location = newDirectory

            # Setting a working directory
            if "workingDirectory" in product.keys():
                scratch_folder = product.workingDirectory
                self.outputdirectory = product.workingDirectory
                self.shared_prod_path = os.path.dirname(Config24K.mxdconfig)
                # Sets the product_name to nothing, as this is already in the shared_prod_path variable
                product_name = ""
            else:
                scratch_folder = arcpy.env.scratchFolder
                # Gets the Product Name
                product_name = product.productName
            
            scratch_folder = newDirectory
            arcpy.AddMessage("The scratch_folder is {}.".format(scratch_folder))
            
            # I don't think we need this any longer.
            #hostname = open(Config24K.hostnamepath, 'r')
            #arcpy.AddMessage("The hostname is {}.".format(hostname))
            
            if "mapExportGUID" not in product.keys():
                product.mapExportGUID = "DevRun"
            arcpy.AddMessage("Creating the map generation log file.")
            log = open(os.path.join(output_location, u'{}_{}.log'.format(u'_ags_MapExport', product.mapExportGUID)), u'w')
            USGS_POD_Utilities.logfilewrite(log, "Running task from {} environment.".format(Config24K.versionNameFlag), "High")
            USGS_POD_Utilities.logfilewrite(log, "Hostname: {}".format(Config24K.hostnamepath), "High")
            USGS_POD_Utilities.logfilewrite(log, u'Running the Map Generation Process for map {}.'.format(map_name), "High")
            USGS_POD_Utilities.logfilewrite(log, "Product JSON has been received from the inputes.")
            USGS_POD_Utilities.logfilewrite(log, "---------------Product Inputs--------------")
            USGS_POD_Utilities.jsontolog(log, product)
            USGS_POD_Utilities.logfilewrite(log, "---------------Configuration Settings--------------")
            USGS_POD_Utilities.configstolog(log, USGS_POD_Utilities)
            USGS_POD_Utilities.logfilewrite(log, "---------------End of Configuration Settings--------------")
            
            arcpy.env.overwriteOutput = True
            exportStartTimeStamp = datetime.datetime.now()
            # the t1 variable is used as varaible names in the python code.
            t1 = time.time()
            USGS_POD_Utilities.logfilewrite(log, " is Start Time", "High")

            #Paths to the ArcGIS Scratch workspaces            
            scratchWorkspaceName = "_ags_ScatchDatabase.gdb"
            arcpy.CreateFileGDB_management(scratch_folder, scratchWorkspaceName)
            scratch_workspace = os.path.join(scratch_folder, scratchWorkspaceName)
            USGS_POD_Utilities.logfilewrite(log, "The Scratch Workspace is: {}.".format(scratch_workspace))
            

            # uncomment code for debugging in python IDEs
            arcpy.CheckExtension("foundation")
            arcpy.CheckOutExtension("foundation") 
            arcpy.CheckOutExtension("Spatial")

            # Makes sure the output directory exists
            if not arcpy.Exists(self.outputdirectory):
                arcpy.AddError(self.outputdirectory + " doesn't exist")
                raise arcpy.ExecuteError

            # Gets the geometry for the AOI
            if product.geometry == "":
                arcpy.AddError("Geometry Object can't be NULL.")
                raise arcpy.ExecuteError
            if "batchprocessing" not in product.keys():
                aoi = arcpy.AsShape(json.dumps(product.geometry), True)
            else:
                if product.batchprocessing == True:
                    aoi = arcpy.AsShape(product.geometry, True)
                else:
                    aoi = arcpy.AsShape(json.dumps(product.geometry), True)
            if "email" in product.keys():
                userEmailAddress = product.email
            else:
                userEmailAddress = None

            if product.cell_id == -9999999:
                product.cell_id = ""
                customorgrid = "Custom Selected"
            else:
                customorgrid = "Grid Selected Area"  
                
            mapScale = 24000
                
            removeLayers = []
            removeLabels = []
            if "layersToRemove" in product.keys():
                
                if product.layersToRemove != []:
                    s1 = datetime.datetime.now()
                    USGS_POD_Utilities.logfilewrite(log, "------------")
                    USGS_POD_Utilities.logfilewrite(log, "Starting the process to determine which layers to remove.")    
                    
                    removeLayers, removeLabels = USGS_POD_Utilities.determineLayersToRemove(product.layersToRemove, Config24K, log)
                    
                    USGS_POD_Utilities.logfilewrite(log, "The following layers will be removed from the Map Document: {}.".format(removeLayers))
                    USGS_POD_Utilities.logfilewrite(log, "The following labels will be removed from the TNM Derived Names Layer: {}.".format(removeLabels))

                    if 'Geographic Names' in product.layersToRemove and 'Hydrography' in product.layersToRemove and 'Wetlands' in product.layersToRemove:
                        (removeLayers).append('TNM Derived Names')                    
                    
                    s2 = datetime.datetime.now()
                    USGS_POD_Utilities.logfilewrite(log, "Finsihed the process to determine which layers to remove, process took {}.".format(str(s2-s1)))   
                    USGS_POD_Utilities.logfilewrite(log, "------------")
            else:
                product.layersToRemove = []
            s1 = datetime.datetime.now()
            USGS_POD_Utilities.logfilewrite(log, "Starting the process to create the AOI Feature Class.")

            aoi_sr = aoi.spatialReference

            # Creates a temporary AOI feature class
            custom_aoi_fc = arcpy.CreateFeatureclass_management(scratch_workspace,
                                                                "Custom_Map_AOI",
                                                                "POLYGON",
                                                                "",
                                                                "",
                                                                "",
                                                                aoi_sr)
            arcpy.AddField_management(custom_aoi_fc, "cell_name", "TEXT")
            
            editSW = arcpy.da.Editor(scratch_workspace)
            editSW.startEditing(False, False)
            editSW.startOperation()            
            
            # Creates the AOI feature in the AOI Feature class
            insert_fields = ['SHAPE@', "cell_name"]
            in_cur = arcpy.da.InsertCursor(custom_aoi_fc, insert_fields)
            in_cur.insertRow([aoi, product.mapSheetName])
            
            editSW.stopOperation()
            editSW.stopEditing(True)     
            del editSW

            #Projects AOI to NAD 83
            custom_aoi_fc_project = os.path.join(scratch_workspace, "customAOIfcproject")
            arcpy.Project_management(custom_aoi_fc, custom_aoi_fc_project, "GEOGCS['GCS_North_American_1983',DATUM['D_North_American_1983',SPHEROID['GRS_1980',6378137.0,298.257222101]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]]", 'WGS_1984_(ITRF00)_To_NAD_1983', "PROJCS['WGS_1984_Web_Mercator_Auxiliary_Sphere',GEOGCS['GCS_WGS_1984',DATUM['D_WGS_1984',SPHEROID['WGS_1984',6378137.0,298.257223563]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]],PROJECTION['Mercator_Auxiliary_Sphere'],PARAMETER['False_Easting',0.0],PARAMETER['False_Northing',0.0],PARAMETER['Central_Meridian',0.0],PARAMETER['Standard_Parallel_1',0.0],PARAMETER['Auxiliary_Sphere_Type',0.0],UNIT['Meter',1.0]]", 'NO_PRESERVE_SHAPE', '#', 'NO_VERTICAL')


            # Makes the Temp AOI FC a layer
            custom_aoi_layer = arcpy.MakeFeatureLayer_management(custom_aoi_fc_project, "Custom_AOI_Index")
            custom_aoi_lyr = None
            for aoi_instance in custom_aoi_layer:
                if aoi_instance.name == "Custom_AOI_Index":
                    custom_aoi_lyr = aoi_instance
            del in_cur
            
            s2 = datetime.datetime.now()
            USGS_POD_Utilities.logfilewrite(log, "Finished Creating the AOI Featrue Class and Layer, process took {}.".format(str(s2-s1)))
            USGS_POD_Utilities.logfilewrite(log, "------------")
            
            
            s1 = datetime.datetime.now()
            USGS_POD_Utilities.logfilewrite(log, "Extracting the data to an File GDB.")
            
            # PSE - can we remove initial variable declaration since all loop paths below initialize it?
            # might not be needed, would need to remove and try, low priority given refactor freeze
            contoursExist = None
            outputDatabase = USGS_POD_Utilities.unzipfile(Config24K.contour_database, scratch_folder, log)
            newName = "_ags_" + outputFolderName
            newContourdatabase = os.path.join(scratch_folder, newName)
            arcpy.Rename_management(outputDatabase, newContourdatabase)
            outputDatabase = newContourdatabase + ".gdb"
            del newContourdatabase, newName  

            aoibufferFC = os.path.join(scratch_workspace, "aoibuffer")
            aoibufferFCProject = os.path.join(scratch_workspace, "aoibufferFCProject")

            arcpy.Buffer_analysis(custom_aoi_layer, aoibufferFC,'6000 Meters')

            arcpy.Project_management(aoibufferFC, aoibufferFCProject, "PROJCS['WGS_1984_Web_Mercator_Auxiliary_Sphere',GEOGCS['GCS_WGS_1984',DATUM['D_WGS_1984',SPHEROID['WGS_1984',6378137.0,298.257223563]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]],PROJECTION['Mercator_Auxiliary_Sphere'],PARAMETER['False_Easting',0.0],PARAMETER['False_Northing',0.0],PARAMETER['Central_Meridian',0.0],PARAMETER['Standard_Parallel_1',0.0],PARAMETER['Auxiliary_Sphere_Type',0.0],UNIT['Meter',1.0]]", 'WGS_1984_(ITRF00)_To_NAD_1983', "GEOGCS['GCS_North_American_1983',DATUM['D_North_American_1983',SPHEROID['GRS_1980',6378137.0,298.257222101]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]]", 'NO_PRESERVE_SHAPE', '#', 'NO_VERTICAL')
            
            inputDatasetsArray = Config24K.inputDatasets
            sdeDatabaseConnection = Config24K.vector_db_connection

            USGS_POD_Utilities.extractData(inputDatasetsArray, sdeDatabaseConnection, outputDatabase, aoibufferFCProject, log)
            
            del inputDatasetsArray
            
            s2 = datetime.datetime.now()
            USGS_POD_Utilities.logfilewrite(log, "Finishing extracting the data to an File GDB, process took {}.".format(str(s2-s1)))
            USGS_POD_Utilities.logfilewrite(log, "------------")
            
            interval = None
            # PSE - flagged for refactoring --- don't touch until 100k changes are merged, then revisit
            if "Contours" not in removeLayers or "USTopoShadedRelief" not in removeLayers:
                
                USGS_POD_Utilities.logfilewrite(log, "Starting the process to Download the DEMs for this AOI.")
                
                if arcpy.Exists(Config24K.raster_source) != True:
                    arcpy.AddError("Can't access the raster source: {}.".format(Config24K.raster_source))
                    raise arcpy.ExecuteError()
                    
                clippedDEM, projectedDEM = ContourGeneration.DownloadRaster(Config24K.raster_source, Config24K.rasterSourceSql, aoibufferFCProject, scratch_folder, log)
                USGS_POD_Utilities.logfilewrite(log, "Finished the process to Download the DEMs for this AOI.")
                
                
                if "Contours" not in removeLayers:
                    
                    USGS_POD_Utilities.logfilewrite(log, "Starting the process to Hydro Enforce the DEM.")
                    mergedFeatureClass = DEMHydroEnforcement.MergeHydroFeatureClasses(outputDatabase, scratch_workspace, Config24K, log)
                    
                    mergedClippedHydro = os.path.join(scratch_workspace, "mergedClippedHydro")
                    arcpy.Clip_analysis(mergedFeatureClass, aoibufferFCProject, mergedClippedHydro)
                                        
                    hydroEnforcedDEM = DEMHydroEnforcement.HydroEnforceDEM(clippedDEM, mergedClippedHydro, scratch_workspace, scratch_folder, log)
                    
                    USGS_POD_Utilities.logfilewrite(log, "Finished the process to Hydro Enforce the DEM.")
                    
                    s1 = datetime.datetime.now()
                    USGS_POD_Utilities.logfilewrite(log, "Starting the process to Create the Contours.")
                    
                    # PSE - what does GeneratorContours return for contoursExist and why does it need to be reassigned to True?
                    # needs more research

                    interval, contoursExist = ContourGeneration.GeneratorContours(product_name, aoibufferFCProject, hydroEnforcedDEM, product.cell_id, scratch_workspace, scratch_folder, Config24K.contour_interval_feature_class, product.contourSmoothingScale, outputDatabase, Config24K.contourIntervalLookUp, projectedDEM, log)
                    
                    contoursExist = True
                else:
                    contoursExist = False
            else:
                contoursExist = False
                                        
                    

            s2 = datetime.datetime.now()
            USGS_POD_Utilities.logfilewrite(log, "Finished Creating Contours, process took {}.".format(str(s2-s1)), "High")
            USGS_POD_Utilities.logfilewrite(log, "------------")        
            #-------------------------------------------------------------------------------------------------------------------
            del sdeDatabaseConnection
            
            s1 = datetime.datetime.now()
            USGS_POD_Utilities.logfilewrite(log, "Starting the process to select the correct MXD based on location.", "High")

            # Gets the product folder
            product_location = os.path.join(self.shared_prod_path, product_name)

            # Determines if the quad is over USFS lands or USGS
            usfs_fc = os.path.join(Config24K.vector_db_connection, "tnmvector.gu.GU_Reserve") ##USGS Integration
            usfs_layer = arcpy.MakeFeatureLayer_management(usfs_fc, "USFS_Layer", "FTYPE = 671 OR FTYPE = 672")

            arcpy.SelectLayerByLocation_management(usfs_layer, 'INTERSECT', custom_aoi_layer, '#', 'NEW_SELECTION', 'NOT_INVERT')
            quad_jurisdiction = "USGS"
            with arcpy.da.SearchCursor(usfs_layer, ["NAME"]) as s_cursor:
                for row in s_cursor:
                    USGS_POD_Utilities.logfilewrite(log, "The AOI is over " + row[0] + ".")
                    quad_jurisdiction = "USFS"

            # Determine which State(s) the AOI is over.
            cell_fc = os.path.join(Config24K.vector_db_connection, "tnmvector.cells.CellGrid_7_5Minute")  ##USGS Integration
            cell_layer = arcpy.MakeFeatureLayer_management(cell_fc, "Cell_Layer")

            arcpy.SelectLayerByLocation_management(cell_layer, 'INTERSECT', custom_aoi_layer, '#', 'NEW_SELECTION', 'NOT_INVERT')
            state = None
            with arcpy.da.SearchCursor(cell_layer, ['PRIMARY_STATE']) as s_cursor:
                for row in s_cursor:
                    state = row[0]
                    break

            # Gets the mxd path based off of state and Quad ownership (USGS or USFS)
            mapScale = None
            if not arcpy.Exists(os.path.dirname(Config24K.mxdconfig)):

                # CONUS MXD
                if state not in ('American Samoa', 'Federated States of Micronesia', 'Guam', 'Northern Mariana Islands', 'Puerto Rico', 'Republic of Palau', 'Virgin Islands', 'Alaska'):

                    grid_xml = os.path.join(product_location, Config24K.gridXmlconfig)
                    mxd_path = os.path.join(product_location, Config24K.mxdconfig)
                    mapScale = 24000
                    
                # PR MXD
                elif state in ('Puerto Rico', 'Virgin Islands'):
                    grid_xml = os.path.join(product_location, Config24K.gridXml_20config)
                    mxd_path = os.path.join(product_location, Config24K.mxd_prconfig)
                    mapScale = 20000
                    
                # AK MXD
                elif state in ('Alaska'):
                    grid_xml = os.path.join(product_location, Config24K.gridXml_25config)
                    mxd_path = os.path.join(product_location, Config24K.mxd_akconfig)
                    mapScale = 25000
                    
                # Pacific Territories MXD
                elif state in ('American Samoa', 'Federated States of Micronesia', 'Guam', 'Northern Mariana Islands', 'Republic of Palau'):
                    grid_xml = os.path.join(product_location, Config24K.gridXml_20config)
                    mxd_path = os.path.join(product_location, Config24K.mxd_ptconfig)
                    mapScale = 20000
                    
            else:
                mxd_path = Config24K.mxdconfig
                grid_xml = Config24K.gridXmlconfig

            #del state

            # Validates the template mxd exists
            if arcpy.Exists(mxd_path) != True:
                arcpy.AddError(mxd_path + " doesn't exist at " + os.path.join(self.shared_prod_path, product_name) + ".")
                raise arcpy.ExecuteError

            # Validates the grid XML exists
            if arcpy.Exists(grid_xml) != True:
                arcpy.AddError(grid_xml + " doesn't exist at " + os.path.join(self.shared_prod_path, product_name) + ".")
                raise arcpy.ExecuteError

            map_doc_name = map_name + "_" + date.strftime("%Y%m%d_%H%M%S")

            USGS_POD_Utilities.logfilewrite(log, "Creating the map for the " + map_name + " aoi...", "High")
            # PSE - do we need mxd_backup if it's set in-code? --- Needs rewrite eventually, low priority
            # given refactor freeze, but don't need mxd_backup or subsequent loops
            mxd_backup = True
            
            USGS_POD_Utilities.logfilewrite(log, "Creating the final MXD object.", "High")

            # Gets the mxd object
            # Creates the AOI specific mxd in the scratch location, if keeping backup copies
            final_mxd_path = None
            if mxd_backup == True:
                final_mxd_path = os.path.join(scratch_folder, "_ags" + map_doc_name + ".mxd")
                USGS_POD_Utilities.logfilewrite(log, "MXD path is: " + final_mxd_path, "High")
                shutil.copy(mxd_path, final_mxd_path)
                del mxd_path
                final_mxd = arcpy.mapping.MapDocument(final_mxd_path)
            else:
                #Creates the mxd object from the template mxd, if not saving backup copies
                final_mxd = arcpy.mapping.MapDocument(mxd_path)
                del mxd_path

            USGS_POD_Utilities.logfilewrite(log, "Finished creating the final MXD object.", "High")
            
            
            USGS_POD_Utilities.logfilewrite(log, "Resourcing the MXD to the Job Database.")
            
            final_mxd.replaceWorkspaces(Config24K.sampleDataSource, "FILEGDB_WORKSPACE", outputDatabase, "FILEGDB_WORKSPACE", validate=True)
            final_mxd.save()
            
            USGS_POD_Utilities.logfilewrite(log, "Finsihed resourcing the MXD to the Job Database.", "High")
            
            s2 = datetime.datetime.now()
            USGS_POD_Utilities.logfilewrite(log, "Finished getting the correct MXD and creating the MXD Object, process took {}.".format(str(s2-s1)), "High")

            USGS_POD_Utilities.logfilewrite(log, "------------")
            s1 = datetime.datetime.now()
            USGS_POD_Utilities.logfilewrite(log, "Starting the process to get the largest dataframe.")

            # Gets the largest data frame (page size not data frame extent)
            data_frame = arcpy.mapping.ListDataFrames(final_mxd, "Map Layers")[0]
            t2 = time.time()
            USGS_POD_Utilities.logfilewrite(log, "After Prep time is: " + str(t2)) 
            USGS_POD_Utilities.logfilewrite(log, "Time difference is: " + (str(t2-t1)))
            
            s2 = datetime.datetime.now()
            USGS_POD_Utilities.logfilewrite(log, "Finished getting the largest dataframe, process took {}.".format(str(s2-s1)))
            USGS_POD_Utilities.logfilewrite(log, "------------")            

            # Code to generate a Preview for the POD wed app
            if product.exportOption == 'Preview':

                t3 = time.time()
                USGS_POD_Utilities.logfilewrite(log, "After Layer loop time is: " + str(t3)) 
                USGS_POD_Utilities.logfilewrite(log, "Time difference is: " + (str(t3-t2)))   

                grid = arcpyproduction.mapping.Grid(grid_xml)
                new_aoi = aoi.projectAs(grid.baseSpatialReference.GCS)

                data_frame.spatialReference = grid.baseSpatialReference.GCS
                data_frame.panToExtent(new_aoi.extent)
                arcpyproduction.mapping.ClipDataFrameToGeometry(data_frame, new_aoi)
                USGS_POD_Utilities.logfilewrite(log, "data_frame.extent = " + str(data_frame.extent))

                # Full-size export
                preview_name = "_ags_" + map_doc_name + "_preview.jpg"
                preview_path = os.path.join(self.outputdirectory, preview_name)
                arcpy.mapping.ExportToJPEG(final_mxd, preview_path, resolution=25, jpeg_quality=25)
                parameters[1].value = preview_name
                t4 = time.time()
                USGS_POD_Utilities.logfilewrite(log, "After Export time is: " + str(t4)) 
                USGS_POD_Utilities.logfilewrite(log, "Time difference is: " + (str(t4-t3)))

                # copies the Preview image into a folder for the USGS to access.
                shutil.copy(preview_path, os.path.join(Config24K.previewFolder, preview_name))
                parameters[1].value = os.path.join(Config24K.previewFolder, preview_name)
                return

            elif product.exportOption == 'Export':
                
                USGS_POD_Utilities.logfilewrite(log, "------------")
                s1 = datetime.datetime.now()
                USGS_POD_Utilities.logfilewrite(log, "Starting the process the Grid and Graticules Creation Process.")
                
                #Variables required for the script
                utm_zone_fc = "UTMZones_WGS84"
                grid_fds_name = "Grids_WGS84" + date.strftime("%Y%m%d_%H%M%S")

                # Creates a grid object
                grid = arcpyproduction.mapping.Grid(grid_xml)
                if arcpy.Exists(os.path.join(scratch_workspace, grid_fds_name)):
                    #Checks the FDS to insure a grid with the same name doesn't exist
                    arcpy.AddWarning(grid_fds_name + " already exists, deleting the existing Feature Dataset.")
                    arcpy.Delete_management(os.path.join(scratch_workspace, grid_fds_name))

                # Projecting the AOI to the correct GCS
                aoi = aoi.projectAs(grid.baseSpatialReference.GCS)

                # Creating the Feature Dataset for the grid
                USGS_POD_Utilities.logfilewrite(log, "Creating the Feature Dataset for the Grid...")
                grid_fds = arcpy.CreateFeatureDataset_management(scratch_workspace, grid_fds_name, grid.baseSpatialReference.GCS)
                gfds = str(grid_fds)

                #Determines if the aoi over laps UTM Zones
                coord_system_file_gdb = Config24K.coordinateSystemZones

                # Checks to see if the CoodinateSystemZones.gdb is in the Product Location
                # If not it will be extracted from the install location.
                if os.path.exists(coord_system_file_gdb) != True:
                    USGS_POD_Utilities.logfilewrite(log, "The CoordinateSystemZones.gdb doesn't exist in %s. The database will be extracted." %os.path.join(self.shared_prod_path,
                                                                                                                                        product_name,
                                               coord_system_file_gdb), "High")
                    install_location = arcpy.GetInstallInfo()['InstallDir']
                    zipped_file = os.path.join(install_location, r"GridTemplates\ProductionMapping", "CoordinateSystemZones.zip")
                    z = zipfile.ZipFile(zipped_file)
                    z.extractall(os.path.join(self.shared_prod_path, product_name))

                    USGS_POD_Utilities.logfilewrite(log, "CoordinateSystemZones.gdb extracted successfully at %s." %os.path.join(self.shared_prod_path, product_name), "High")

                temp_fc = os.path.join(coord_system_file_gdb, utm_zone_fc)
                utm_lyr = arcpy.mapping.Layer(temp_fc)
                arcpy.SelectLayerByLocation_management(utm_lyr, "INTERSECT", aoi)
                result = arcpy.GetCount_management(utm_lyr)
                zone_count = int(result.getOutput(0))

                USGS_POD_Utilities.logfilewrite(log, "Checking for overlapping UTM Zones..")
                # Checking for unique UTM zones in selection set
                if zone_count > 1:
                    # PSE - flagged for potential refactor
                    zone_num = {}
                    with arcpy.da.SearchCursor(utm_lyr, ["OBJECTID", "ZONE_NUM"]) as zone_cursor:
                        for row in zone_cursor:
                            zone_num[row[1]] = row[0]
                    if len(zone_num) != zone_count:
                        zone_count = len(zone_num)
                    del zone_cursor, zone_num
                del utm_lyr

                # Once we have a zipper xml, replace the defualt on with zipper.
                if zone_count > 1 and product.cell_id == "": # Use zipper grid XML
                    grid_xml = os.path.join(self.shared_prod_path, product_name, Config24K.gridXml_zipperconfig)

                #Uses the appropriate XML for to create the grid
                USGS_POD_Utilities.logfilewrite(log, "Creating the Grid...")
                output_layer = map_name + '_' + grid.type
                output_layer_new = re.sub('[^a-zA-Z0-9 \n\.]', '', output_layer)
                map_name_new = re.sub('[^a-zA-Z0-9 \n\.]', '', map_name)
                grid_result = arcpy.MakeGridsAndGraticulesLayer_cartography(grid_xml, aoi, gfds, output_layer_new, map_name_new)
                USGS_POD_Utilities.logfilewrite(log, grid_result.getMessages())
                grid_layer = grid_result.getOutput(0)

                # Updates the current map document using grid object and methods
                # Add/Update the grid layer to the top of the map
                layers = arcpy.mapping.ListLayers(final_mxd, grid_layer.name, data_frame)
                if len(layers) > 0:
                    arcpy.AddWarning("Grid Layer "+ grid_layer.name + " already exists in the map. It will be updated to use the new grid.")
                    for layer in layers:
                        arcpy.mapping.RemoveLayer(data_frame, layer)
                    arcpy.RefreshTOC()

                arcpy.mapping.AddLayer(data_frame, grid_layer, "TOP")
                USGS_POD_Utilities.logfilewrite(log, "Grid Layer added to map...")

                # Updates the data frame properties base on the grid
                final_mxd.activeView = 'PAGE_LAYOUT'
                grid.updateDataFrameProperties(data_frame, aoi)

                USGS_POD_Utilities.logfilewrite(log, "Dataframe's extent is now = " + str(data_frame.extent), "High")

                # Masks the ladder values i.e. interior grid
                # annotion that overlap the grid line.
                anno_layer = arcpy.mapping.ListLayers(final_mxd, "ANO_*", data_frame)[0]
                gridline_layer = arcpy.mapping.ListLayers(final_mxd, "GLN_*", data_frame)[0]

                # Returns a new mask feature class for masking the
                # gridlines that intersect interior annotation or ladder values
                annomask_ctr = 1
                workspace = os.path.dirname(gfds)
                for directory, dirnames, filenames in arcpy.da.Walk(workspace,
                                                                    datatype="FeatureClass",
                                                                    type="Polygon"):
                    for filename in filenames:
                        if filename.find("AnnoMask") > -1:
                            annomask_ctr = annomask_ctr +1
                annomask_fc = gfds + "\\AnnoMask_" + str(annomask_ctr)

                if not arcpy.env.overwriteOutput and arcpy.Exists(annomask_fc):
                    arcpy.AddError(annomask_fc + "already exists. Cannot create output.")
                USGS_POD_Utilities.logfilewrite(log, "making the feature outline masks")
                masks = arcpy.FeatureOutlineMasks_cartography(anno_layer, annomask_fc,
                                                              grid.scale, grid.baseSpatialReference.GCS,
                                                              '2.5 Points', 'CONVEX_HULL',
                                                              'ALL_FEATURES')
                # Masking the grid ladder values and annotations
                USGS_POD_Utilities.logfilewrite(log, "getting output of masks.")
                anno_mask_layer = arcpy.mapping.Layer(masks.getOutput(0))
                arcpy.mapping.AddLayer(data_frame, anno_mask_layer, 'BOTTOM')
                anno_mask = arcpy.mapping.ListLayers(final_mxd, anno_mask_layer.name, data_frame)[0]
                USGS_POD_Utilities.logfilewrite(log, "Annotation Mask '" + anno_mask.name + "' layer added to the map...")
                arcpyproduction.mapping.EnableLayerMasking(data_frame, 'true')
                arcpyproduction.mapping.MaskLayer(data_frame, 'APPEND', anno_mask, gridline_layer)
                USGS_POD_Utilities.logfilewrite(log, "Masking applied to gridlines...")

                #Clips the Data Frame to the AOI and exculdes the Grid
                gird_components = arcpy.mapping.ListLayers(final_mxd, grid_layer, data_frame)
                arcpyproduction.mapping.ClipDataFrameToGeometry(data_frame, aoi, gird_components)
                
                mainDFScale = str(int(data_frame.scale))

                s2 = datetime.datetime.now()
                USGS_POD_Utilities.logfilewrite(log, "FinishedCreating the Grid and Graticules and updating the MXD, process took {}.".format(str(s2-s1)), "High")
                

                USGS_POD_Utilities.logfilewrite(log, "------------")
                s1 = datetime.datetime.now()
                USGS_POD_Utilities.logfilewrite(log, "Updating the ASG and State Location Diagrams.")
              

                #Getting the list of data frames
                data_frame_list = arcpy.mapping.ListDataFrames(final_mxd)
                # PSE - flagged for potential refactor
                quadrangle_location = None
                adjoining_quad_data_frame = None
                for b_data_frame in data_frame_list:
                    if b_data_frame.name == "Quadrangle Location":
                        quadrangle_location = b_data_frame
                    elif b_data_frame.name == "Adjoining Sheet Diagram":
                        adjoining_quad_data_frame = b_data_frame

                

                # Makes the mask layer invisible and prepares map for save
                anno_mask.visible = "false"
                #Logic for update the adjoining sheet and location diagrams

                # Gets the Map AOI Layer from the MXD
                map_aoi_layer = None
                blm_layer = None
                plss_layer = None
                airportRunwaysLayer = None
                airportPointLayer = None
                layers = arcpy.mapping.ListLayers(final_mxd, "", data_frame)
                for layer in layers:
                    if layer.name == "Map_AOI":
                        map_aoi_layer = layer
                        break
                    #elif layer.name == "Orthoimage":
                        #layer.visible = True
                    elif layer.name == "Bureau of Land Management":
                        blm_layer = layer
                    elif layer.name == "PLSS":
                        plss_layer = layer
                    elif layer.name == "Airport Point":
                        airportPointLayer = layer
                    elif layer.name == "Airport Runway":
                        airportRunwaysLayer = layer                        
                        
                del layers, layer
                

                
                #Get airport Runways Label into
                airpotClassName = None
                aiprotClassExpression = None
                airportLabelClasses = airportRunwaysLayer.labelClasses
                for airportLabel in airportLabelClasses:
                    airpotClassName = airportLabel.className
                    aiprotClassExpression = airportLabel.expression
                                  
                
                # Updating the Airport Runway Join
                airPortLayersDefQuery = airportRunwaysLayer.definitionQuery
                arcpy.RemoveJoin_management(airportRunwaysLayer, 'Trans_AirportPoint')
                arcpy.AddJoin_management(airportRunwaysLayer, 'faa_airport_code', airportPointLayer, 'faa_airport_code', 'KEEP_ALL')
                airportRunwaysLayer.definitionQuery = airPortLayersDefQuery
                del airPortLayersDefQuery
                
                #Reset the label class
                airportLabelClasses = airportRunwaysLayer.labelClasses
                for airportLabel in airportLabelClasses:
                    airportLabel.className = airpotClassName
                    airportLabel.expression  = aiprotClassExpression               
                

                # Updates the Quadrangle Location diagram information
                layers = arcpy.mapping.ListLayers(final_mxd, "", quadrangle_location)
                index_aoi = None
                us_states = None

                for layer in layers:
                    if layer.name == "Index_AOI":
                        index_aoi = layer
                    elif layer.name == "State Outline":
                        us_states = layer
                del layers

                # Add the correct symbology for the layer
                arcpy.ApplySymbologyFromLayer_management(custom_aoi_layer, index_aoi)
                # Adding the custom AOI Layer
                arcpy.mapping.AddLayer(quadrangle_location, custom_aoi_lyr, "TOP")

                # Removing the current AOI Index Layer
                arcpy.mapping.RemoveLayer(quadrangle_location, index_aoi)

                # For standard quads, get the primary state info
                # For custom quads, determine the stateoverlap dynamically

                state_name = None
                state_extent = None
                stateFipsCode = None

                if product.cell_id == "":
                    # Removing Definition querry on US States
                    us_states.definitionQuery = ""
                    
                    usStatesFC = Config24K.usStatesFeatureClassPath
                    
                    USStatesLayer = arcpy.MakeFeatureLayer_management(usStatesFC, "USStatesLayer")

                    # Determines which state this AOI is interesting with
                    arcpy.SelectLayerByLocation_management(USStatesLayer, 'INTERSECT', custom_aoi_lyr, '#', 'NEW_SELECTION', 'NOT_INVERT')

                    # Gets the State information
                    # this is reguired as the location diagram is only to show 1 state, not multiple
                    with arcpy.da.SearchCursor(USStatesLayer, ["SHAPE@", "STATE_NAME", "state_fipscode"]) as s_cursor:
                        for row in s_cursor:
                            state_name = row[1]
                            #state_extent = row[0].extent
                            stateFipsCode = row[2]
                            break
                        
                    arcpy.Delete_management(USStatesLayer)
                    del usStatesFC, USStatesLayer

                    us_states.definitionQuery = "state_fipscode = '" + str(stateFipsCode) + "'"
                    
                    with arcpy.da.SearchCursor(us_states, ["SHAPE@"]) as s_cursor:
                        for row in s_cursor:
                            state_extent = row[0].extent
                            break                     

                elif str(product.primary_state) == "Virgin Islands":
                    state_name = "Virgin Islands"
                    us_states.definitionQuery = "STATE_NAME = '" + str(state_name) + "'"

                    with arcpy.da.SearchCursor(us_states, ["SHAPE@"]) as s_cursor:
                        for row in s_cursor:
                            state_extent = row[0].extent
                            break    
                else:
                    state_name = str(product.primary_state)
                    us_states.definitionQuery = "STATE_NAME = '" + str(state_name) + "'"

                    with arcpy.da.SearchCursor(us_states, ["SHAPE@"]) as s_cursor:
                        for row in s_cursor:
                            state_extent = row[0].extent
                            break
                arcpy.SelectLayerByAttribute_management(us_states, 'CLEAR_SELECTION', '#')
                
                # Updating the States Layer with the correct State
                if state_name.upper() == "ALASKA":
                    quadrangle_location.panToExtent(us_states.getExtent())
                    quadrangle_location.scale = 260000000

                    new_extent = quadrangle_location.extent
                    new_extent.XMax, new_extent.XMin  = 6117397.117812835, -1549862.216706222
                    new_extent.YMax, new_extent.YMin = 11765622.650830258, 6263819.247224564
                    quadrangle_location.extent = new_extent
                    #arcpy.RefreshActiveView()
                    del new_extent

                else:
                    quadrangle_location.extent = state_extent
                    quadrangle_location.scale = quadrangle_location.scale * 1.10
                USGS_POD_Utilities.logfilewrite(log, "Updated the Quadrangle Location Data Frame...")

                ####--------USGS Code for the adjoining sheet diragram


                # Updates the Adjoining Quad Diagram
                # Gets the list of layers for the adjoining quads dataframe
                adjoining_quad_layer_list = arcpy.mapping.ListLayers(final_mxd, "", adjoining_quad_data_frame)
                adjoing_quad_aoi_layer = None
                quad_aoi_layer = None
                for adjoining_layer in adjoining_quad_layer_list:
                    if adjoining_layer.name == "Index_AOI":
                        adjoing_quad_aoi_layer = adjoining_layer
                    elif adjoining_layer.name == "Map_AOI":
                        quad_aoi_layer = adjoining_layer
                del adjoining_quad_layer_list
                
                arcpy.ApplySymbologyFromLayer_management(custom_aoi_lyr, adjoing_quad_aoi_layer)
                #arcpy.mapping.InsertLayer(adjoining_quad_data_frame, adjoing_quad_aoi_layer, newCustomLayer, "BEFORE")
                arcpy.mapping.AddLayer(adjoining_quad_data_frame, custom_aoi_lyr, "TOP")
                arcpy.mapping.RemoveLayer(adjoining_quad_data_frame, adjoing_quad_aoi_layer)
                
                arcpy.Delete_management(adjoing_quad_aoi_layer)

                arcpy.SelectLayerByLocation_management(quad_aoi_layer, "INTERSECT", custom_aoi_lyr, "#", "NEW_SELECTION", "NOT_INVERT")
    
                # Determins which Quad the Custom Extent AOI center point is over
                # If the center point is on the boundary of more than 1 quad, the first quad will be used
                overlapping_quad_id = None
                quadlist = []
                

                with arcpy.da.SearchCursor(quad_aoi_layer, ["cell_id", "SHAPE@"]) as s_cursor:
                    for row in s_cursor:
                        quadlist.append(str(row[0]))
                        
                arcpy.AddMessage(str(quadlist))
                        
                quad_aoi_layer.definitionQuery = '"CELL_ID" IN ({})'.format(", ".join(quadlist))
                

                adjoining_quad_data_frame.zoomToSelectedFeatures()

                arcpy.SelectLayerByAttribute_management(quad_aoi_layer, 'CLEAR_SELECTION')

                del quad_aoi_layer, adjoing_quad_aoi_layer
                
                arcpy.RefreshActiveView()
                arcpy.RefreshTOC()

                if mxd_backup == True:
                    final_mxd.save()
                    
                s2 = datetime.datetime.now()
                USGS_POD_Utilities.logfilewrite(log, "Finished updating the ASG and State Location Diagram, process took {}.".format(str(s2-s1)))
             

                USGS_POD_Utilities.logfilewrite(log, "------------")
                s1 = datetime.datetime.now()
                USGS_POD_Utilities.logfilewrite(log, "Starting the process to get the FIPSCODESs.")  
                    

                # Getting the a list of state FIPS codes
                # This Search Cursor is required, becase the complete list of states is needed for the map surround elements
                state_fipscode = []
                with arcpy.da.SearchCursor(us_states, ["STATE_FIPSCODE"]) as s_cursor:
                    for row in s_cursor:
                        state_fipscode.append(row[0])

                # Logic to get the additional information to update the surround elements
                # Getting the State information:
                state_abr = ""

                #First Builds the US States and Terriroties Data Dictonary  
                us_states_generalized = os.path.join(Config24K.vector_db_connection, "tnmvector.mapref.GU_States_Generalized")  ##USGS Integration
                us_states_data_matrix = []
                with arcpy.da.SearchCursor(us_states_generalized, ["STATE_ABBR", "STATE_FIPS", "STATE_NAME"]) as s_cursor:
                    for row in s_cursor:
                        us_states_data_matrix.append([row[0], row[1], row[2]])

                # Formatting the state abr string.
                # Logic for standard quads
                if product.cell_id != "":
                    map_aoi_layer.definitionQuery = "CELL_ID = " + str(product.cell_id)
                    with arcpy.da.SearchCursor(map_aoi_layer, ["PRIMARY_STATE", "STATE_ALPHA"]) as s_cursor:
                        for row in s_cursor:
                            state_abr = row[1]
                    state_abr = re.sub(r'([,]+)',r'\1 ', state_abr)
                # Logic for custom quads
                else:
                    # This uses the state_fipscode(3) gathered above to find all states abbrivations
                    for code in state_fipscode:
                        for row in us_states_data_matrix:
                            fipscode = row[1]
                            if fipscode == code:
                                state_abr += row[0] + ", "
                    state_abr = state_abr[:-2]
                del row, s_cursor
                
                s2 = datetime.datetime.now()
                USGS_POD_Utilities.logfilewrite(log, "Finished getting the FIPSCODES, process took {}.".format(str(s2-s1)))          

                USGS_POD_Utilities.logfilewrite(log, "Finalizing the map document...")
                
                USGS_POD_Utilities.logfilewrite(log, "------------")
                s1 = datetime.datetime.now()
                USGS_POD_Utilities.logfilewrite(log, "Starting the process to update the surround elements.")                

                # Determines the Zone and the AOI Insterestction for the Zone information
                utmzones_nad83 = os.path.join(coord_system_file_gdb, "UTMZones_NAD83")

                # Getting the AOI and the UTM zone FC in the same projection
                arcpy.Project_management(utmzones_nad83, os.path.join(scratch_workspace, "UTMZones_NAD83_pro"), aoi_sr,)
                utmzones_nad83_lyr = arcpy.MakeFeatureLayer_management(os.path.join(scratch_workspace, "UTMZones_NAD83_pro"), "utmzones_nad83_lyr")

                count = 0
                utm_zone = []
                utm_row = []

                # Gets the interesting UTM Zones
                arcpy.SelectLayerByLocation_management(utmzones_nad83_lyr, 'INTERSECT', custom_aoi_lyr, '#', 'NEW_SELECTION', 'NOT_INVERT')
                with arcpy.da.SearchCursor(utmzones_nad83_lyr, ["ZONE_", "ROW_"]) as s_cursor:
                    for row in s_cursor:
                        count += 1
                        utm_zone.append(row[0])
                        utm_row.append(row[1])

                utm_text = None
                utm_row_text = ""
                # Creates the UTM String for the layout
                if count == 1:
                    utm_text = "Universal Transverse Mercator, Zone " + str(int(utm_zone[0])) + str(utm_row[0])
                else:
                    for item in range(len(utm_row)):
                        utm_row_text = utm_row_text + str(int(utm_zone[item])) + str(utm_row[item]) + "\\"
                    utm_row_text = utm_row_text[:-1]
                    utm_text = "Universal Transverse Mercator, Zone " + utm_row_text

                # Gets the interesting Counties and States for the AOI - used for the <MAP_TITLE_STATE_NAME_UPPER_RIGHT> Text
                counties_list = []
                state_list = []

                county_fc = os.path.join(###########, "###########") ##USGS Integration
                county_lyr = arcpy.MakeFeatureLayer_management(county_fc, "county_lyr")
                del county_fc
                arcpy.SelectLayerByLocation_management(county_lyr, 'INTERSECT', custom_aoi_lyr, '#', 'NEW_SELECTION', 'NOT_INVERT')
                with arcpy.da.SearchCursor(county_lyr, ["STATE_NAME", "GNIS_NAME"]) as s_cursor:
                    for row in s_cursor:
                        if row[1] not in counties_list:
                            counties_list.append(row[1])
                        if row[0] not in state_list:
                            state_list.append(row[0])
                            
                #Gets the list of layout elements
                layout_elements = arcpy.mapping.ListLayoutElements(final_mxd)     

                # Updating the Surround Elements
                if contoursExist == False:
                    interval = 0
                    #countorSimplificationValue = 0
                    USGS_POD_Utilities.logfilewrite(log, "Contour interval and simplification value has been set to zero")
                    MapGeneratorAutoLabeling.updateLayoutElements(self, layout_elements, originalMapName, state_name, product.mapSheetName, product.cell_id, plss_layer, date, utm_text, counties_list, state_list, state_fipscode, state_abr, us_states_data_matrix, overlapping_quad_id, interval, product.SmoothnessLevel, log)
                else:
                    MapGeneratorAutoLabeling.updateLayoutElements(self, layout_elements, originalMapName, state_name, product.mapSheetName, product.cell_id, plss_layer, date, utm_text, counties_list, state_list, state_fipscode, state_abr, us_states_data_matrix, overlapping_quad_id, interval, product.SmoothnessLevel, log)
                    
                if "Contours" in removeLayers:
                    for element in layout_elements:
                        if element.name == "CONTOUR_INTERVAL":
                            element.delete()
                        if element.name == "CONTOUR_SMOOTHNESS":
                            element.delete()
                elif removeLayers == []:
                    for element in layout_elements:
                        if element.name == "USER DEFINED CONTENT":
                            element.delete()                
                

                # Turns off the Map AOI Layer for the final map
                map_aoi_layer.visible = False
                arcpy.RefreshTOC()
                arcpy.RefreshActiveView()
                
                if mxd_backup == True:
                    final_mxd.save()                

                # Final Output Name:
                finalMapName = ''.join(e for e in map_name if e.isalnum())
                pdf_name = state_abr + "_" + finalMapName + "_" + dateStamp + "_" + timeStamp + "_TM_geo"

                # updates the layout for a USFS 
                if (quad_jurisdiction == "USFS"):
                    # Updating Layers for USFS
                    USGS_POD_Utilities.logfilewrite(log, "Updating the MXD for USFS Quads.")
                    
                    
                    usgsLegend = None
                    usfsLegend = None
                    usfsLogo = None
                    
                    for elm in arcpy.mapping.ListLayoutElements(final_mxd):
                        if elm.name == "USGS_GROUP_ROAD_CLASSIFICATION":
                            usgsLegend = elm
                        elif elm.name == "USFS_GROUP_ROAD_CLASSIFICATION":
                            usfsLegend = elm
                        elif elm.name == "USFS_LOGO":
                            usfsLogo = elm
                    
                    if state == 'Puerto Rico':
                        usfsLogo.elementPositionX = Config24K.usfsLogoXPR
                        usfsLogo.elementPositionY = Config24K.usfsLogoYPR                        
                        
                    else:
                        usfsLogo.elementPositionX = Config24K.usfsLogoX
                        usfsLogo.elementPositionY = Config24K.usfsLogoY
 
                    usfsLegend.elementPositionX = usgsLegend.elementPositionX
                    usfsLegend.elementPositionY = usgsLegend.elementPositionY
  
                    usgsLegend.elementPositionX = 60
                    
                    
                    layerList = arcpy.mapping.ListLayers(final_mxd)
                    for layer in layerList:
                        if layer.name == "Forest Service":
                            layer.visible = True
                            break
                            
                    del layerList
                    USGS_POD_Utilities.logfilewrite(log, "Finished updating the MXD for USFS Quads.")

                arcpy.RefreshTOC()
                arcpy.RefreshActiveView()                

                if mxd_backup == True:
                    final_mxd.save()
                
                s2 = datetime.datetime.now()
                USGS_POD_Utilities.logfilewrite(log, "Finished updating surround elements, process took {}.".format(str(s2-s1)))
                USGS_POD_Utilities.logfilewrite(log, "------------")
                
                if state not in ('American Samoa', 'Federated States of Micronesia', 'Guam', 'Northern Mariana Islands', 'Puerto Rico', 'Republic of Palau', 'Virgin Islands'):
                    USGS_POD_Utilities.logfilewrite(log, "------------")
                    s1 = datetime.datetime.now()
                    USGS_POD_Utilities.logfilewrite(log, "Starting the process to convert the PLSS to annotations.")   
                    
                    finalDataFrame = arcpy.mapping.ListDataFrames(final_mxd, "Map Layers")[0]
                    layerlist = arcpy.mapping.ListLayers(final_mxd, "", finalDataFrame)                    
    
                    # Turn off layers
                    for layer in layerlist:
                        USGS_POD_Utilities.logfilewrite(log,layer.name)
                        if layer.name not in ['Township/Range', 'SpecialSurvey', 'Section', 'PLSS', 'Features']:
                            layer.visible = False 

                    arcpy.RefreshTOC()
                    arcpy.RefreshActiveView()                
    
                    if mxd_backup == True:
                        final_mxd.save()                    

                    final_mxd_path = USGS_POD_Utilities.recreateMxd("_ags" + map_doc_name + "_final.mxd", final_mxd, scratch_folder, log)
                    del final_mxd        
                    
                    # Converts the PLSS Labels to Annotations
                    annotationFeatureClass = os.path.join(outputDatabase, "PLSSAnno")
                    arcpy.defense.ConvertLabelsToAnnotation(final_mxd_path, annotationFeatureClass, str(finalDataFrame.name))
                    
                    final_mxd = arcpy.mapping.MapDocument(final_mxd_path)
                    finalDataFrame = arcpy.mapping.ListDataFrames(final_mxd, "Map Layers")[0]
                    layerlist = arcpy.mapping.ListLayers(final_mxd, "", finalDataFrame)
                    
                    # Turn on layers
                    for layer in layerlist:
                        if layer.name not in ['Map_AOI', 'AnnoMask_1']:
                            layer.visible = True 
                
                    USGS_POD_Utilities.logfilewrite(log, "Finished Resourcing the Annotation Layer to the File GDB.")
                                    
                    s2 = datetime.datetime.now()
                    USGS_POD_Utilities.logfilewrite(log, "Finished converting PLSS to Annotations, process took {}.".format(str(s2-s1)))
                    
                else:
                    finalDataFrame = arcpy.mapping.ListDataFrames(final_mxd, "Map Layers")[0]
                    layerlist = arcpy.mapping.ListLayers(final_mxd, "", finalDataFrame)
                    
                USGS_POD_Utilities.logfilewrite(log, "------------")
                
                
                if "USTopoShadedRelief" not in removeLayers:
                    s1 = datetime.datetime.now()
                    USGS_POD_Utilities.logfilewrite(log, "Starting the process to Create the hillshade.")
                    
                    rasterFolder = os.path.join(output_location, "_ags_RasterData")

                    rasterFolder = USGS_POD_Utilities.CreateHillshade(projectedDEM, rasterFolder, log)

                    s2 = datetime.datetime.now()
                    USGS_POD_Utilities.logfilewrite(log, "Finished creating the hillshade, process took {}.".format(str(s2-s1)))
                    USGS_POD_Utilities.logfilewrite(log, "------------")                           
                
                s1 = datetime.datetime.now()
                USGS_POD_Utilities.logfilewrite(log, "Starting the process to Resource the MXD to the Carto dervied data.")
    
                USGS_POD_Utilities.logfilewrite(log, "The layerList is {}.".format(layerlist))
                
                # PSE - flagged for potential refactor, using removeLayers as possible single source of truth and product.layersToRemove as an initial driver
                if product.layersToRemove != []:
                    # Removing the layers the user has selected to remove:
                    for layer in layerlist:
                        if layer.name in removeLayers:
                            arcpy.mapping.RemoveLayer(finalDataFrame, layer)
                        if layer.name == "TNM Derived Names":
                            if removeLabels != "":
                                defQuery = layer.definitionQuery.strip("\r\n") + " AND gaz_featureclass NOT IN (" + removeLabels + ")"
                                layer.definitionQuery = defQuery
        
                USGS_POD_Utilities.logfilewrite(log, " ")
                
                #USGS_POD_Utilities.repairFileGeodatabase(outputDatabase)
    
                USGS_POD_Utilities.logfilewrite(log, "Reseting the Contour Layer to the File GDB Created Above.")
                # PSE - I wonder if we can combine this if/else with the one above
                if state not in ('American Samoa', 'Federated States of Micronesia', 'Guam', 'Northern Mariana Islands', 'Puerto Rico', 'Republic of Palau', 'Virgin Islands'):
                    for layer in layerlist:
                        if layer.name == 'Contours':
                            if layer.name not in removeLayers:
                                layer.replaceDataSource(outputDatabase, "FILEGDB_WORKSPACE", "Elev_Contour")
                        if layer.name == 'National_PLSS_Anno':
                            if layer.name not in removeLayers:
                                layer.replaceDataSource(outputDatabase, "FILEGDB_WORKSPACE", "PLSSAnno")
                        if layer.name == 'ShadedRelief_5light':
                            if layer.name not in removeLayers:
                                layer.replaceDataSource(rasterFolder, "RASTER_WORKSPACE", "_ags_5light.tif", True)

                else:
                    for layer in layerlist:
                        if layer.name == 'Contours':
                            layer.replaceDataSource(outputDatabase, "FILEGDB_WORKSPACE") 
                        if layer.name == 'ShadedRelief_5light':
                            if layer.name not in removeLayers:
                                layer.replaceDataSource(rasterFolder, "RASTER_WORKSPACE", "_ags_5light.tif", True)                            
                            
                USGS_POD_Utilities.logfilewrite(log, "Finished Resourcing the Layers to the File GDB.")
                
                arcpy.RefreshTOC()
                arcpy.RefreshActiveView()                

                if mxd_backup == True:
                    final_mxd.save()                 

                s2 = datetime.datetime.now()
                USGS_POD_Utilities.logfilewrite(log, "Finished updating the MXD Contour Datasource, process took {}.".format(str(s2-s1)))
                USGS_POD_Utilities.logfilewrite(log, "------------")                    
        
                s1 = datetime.datetime.now()
                USGS_POD_Utilities.logfilewrite(log, "Starting the export process.")                  

                pdf_name = pdf_name.replace(" ", "_")

                USGS_POD_Utilities.logfilewrite(log, "Starting the Export to Process.")
                exportStart = time.time()
                exportStartDateTime = datetime.datetime.now()
                # Export the Map to the selected format
                #retry exporting up to 5 times.  This is only for export.  Retry upon error is now handled by system
                productionexportattempt = 0

                # Gets the correct Exporter
                if product.exporter == "TIFF":
                    if product.cell_id == "":
                        if state_name not in ['Hawaii', 'Virgin Islands', 'Puerto Rico', 'Alaska']:
                            product.exporter = "LAYOUT GEOTIFF"
                    else:
                        if product.primary_state not in ['Hawaii', 'Virgin Islands', 'Puerto Rico', 'Alaska']:
                            product.exporter = "LAYOUT GEOTIFF"
            
                USGS_POD_Utilities.logfilewrite(log, "The exporter is {}.".format(product.exporter), "High")                
                
                finalDataFrame = arcpy.mapping.ListDataFrames(final_mxd, "Map Layers")[0]

                while productionexportattempt < USGS_POD_Utilities.retry:
                    try:
                        if product.exporter == "Production PDF":
                            file_name = USGS_POD_Utilities.export_map_document(product_location, final_mxd,
                                                                               pdf_name, finalDataFrame,
                                                                               output_location, product.exporter, Config24K.productionPDFXMLconfig)
                        else:
                            file_name = USGS_POD_Utilities.export_map_document(product_location, final_mxd,
                                                                               pdf_name, finalDataFrame,
                                                                               output_location, product.exporter)
                        USGS_POD_Utilities.logfilewrite(log, "Production PDF Export complete.", "High")
                        break
                    except:
                        USGS_POD_Utilities.logfilewrite(log, "Trying again")
                        productionexportattempt += 1

                        
                s2 = datetime.datetime.now()
                USGS_POD_Utilities.logfilewrite(log, "Finished exporting to {}, process took {}.".format(product.exporter, str(s2-s1)), "High")                
                
                USGS_POD_Utilities.logfilewrite(log, "------------")
                s1 = datetime.datetime.now()
                USGS_POD_Utilities.logfilewrite(log, "Starting the Metadata XML Update process.")                   


                # Calling the Post Process exe to add the metadata and Legend
                # PSE - flagged for refactoring
                if "batchprocessing" in product.keys():
                    if product.batchprocessing == True:            
                        outfile = os.path.join(output_location,"BatchExport", file_name)

                    else:
                        outfile = os.path.join(output_location, file_name)
                else:
                    outfile = os.path.join(output_location, file_name)

                USGS_POD_Utilities.logfilewrite(log, "The PDF is: " + str(outfile), "High")

                pdf_legend = os.path.join(product_location, Config24K.ustopo_legendconfig)
                USGS_POD_Utilities.logfilewrite(log, "The PDF Lengend PDF is: " + str(pdf_legend), "High")

                post_process = os.path.join(Config24K.post_process_path, "USGSPostProcessPDF.exe")
                USGS_POD_Utilities.logfilewrite(log, "The PDF Post Process Exe is: " + str(post_process))


                pdf_metadata_xml = XMLEditor.editthexml(yearStamp, dateStamp, product.exporter, product.cell_id, originalMapName, state_abr, utm_zone, output_location, product.SmoothnessLevel, Config24K.xml_template_name)
                USGS_POD_Utilities.logfilewrite(log, "The Metadata XML is: " + str(pdf_metadata_xml))

                
                s2 = datetime.datetime.now()
                USGS_POD_Utilities.logfilewrite(log, "Finished updating the Metadata XML, process took {}.".format(str(s2-s1)))                
                

                USGS_POD_Utilities.logfilewrite(log, "Executeing the Post Process Exe")

                if product.exporter == "Production PDF" or product.exporter == "PDF":
                    xml_configuration = os.path.join(###########, product_name, "###########")
                    USGS_POD_Utilities.logfilewrite(log, "The XML Configuration is: " + str(xml_configuration))                    
                    pdfTitle = "{}, {}, {}, Quad, {}, USGS".format(product.mapSheetName, state_abr, mainDFScale, dateStamp)
                    subprocess.check_call([post_process, outfile, xml_configuration, pdfTitle, Config24K.author, "", pdf_metadata_xml, pdf_legend])   
                    
                    
                    USGS_POD_Utilities.logfilewrite(log, "The Post Process Exe is complete.")
                    del xml_configuration
                    
                    # PSE - flagged for refactoring
                    file_name2 = file_name.replace("_ags_","")                 
                    shutil.copy(os.path.join(output_location, file_name), os.path.join(output_location, file_name2))
                    file_name = file_name2

                    pathToMap = os.path.join(Config24K.output_directory, newDirectory, file_name)
                    USGS_POD_Utilities.copyOutputToS3(pathToMap, Config24K.output_bucket, file_name, Config24K.output_bucket_directory, log)
                    finalURL = Config24K.output_bucket_url + "/" + Config24K.output_bucket_directory + file_name   
                    
                else:
                    exepath = Config24K.exiftoolpath

                    imagedescription = originalMapName + " " + state_abr + ", 1:" + str(mapScale) + " Quad, " + str(yearStamp) + ", USGS"    

                    file_name = file_name.replace("_ags_","")
                    file_name = file_name.replace(".tif","")
                    FolderName = file_name      

                    args = [exepath ,"-Title={}".format(imagedescription),"-XPAuthor=U.S. Geological Survey","-Keywords=USGS, Dynamic Mapping", os.path.join(newDirectory, file_name)]
                    subprocess.call(args)    
                    USGS_POD_Utilities.logfilewrite(log, "Creating the Zip File with the extra files.")

                    newDirectory = os.path.join(output_location, FolderName)
                    os.mkdir(newDirectory)
            
                    shutil.copyfile(outfile, os.path.join(newDirectory, file_name + ".tif"))
                    shutil.copyfile(pdf_legend, os.path.join(newDirectory, Config24K.ustopo_legendconfig))
                    metadataloc = os.path.join(newDirectory, "Metadata.xml")
                    shutil.copyfile(pdf_metadata_xml, metadataloc)
                    os.rename(metadataloc, os.path.join(newDirectory, FolderName + ".xml") )
                                            
                    newFileName = file_name[:-4]
                    tempOutfile = os.path.join(scratch_folder, newFileName)
                    outfile = os.path.join(output_location, newFileName + ".zip")
                    #Calls zip function, copies output of zip function to location defined above
                    USGS_POD_Utilities.exporttozip(tempOutfile, newDirectory)
                    shutil.copyfile(tempOutfile, outfile)
                    del tempOutfile

                    pathToMap = os.path.join(Config24K.output_directory, outfile)

                    USGS_POD_Utilities.copyOutputToS3(pathToMap, Config24K.output_bucket, file_name + ".zip", Config24K.output_bucket_directory, log)
                    finalURL = Config24K.output_bucket_url + "/" + Config24K.output_bucket_directory + file_name + ".zip"
                    
                arcpy.Delete_management(pathToMap)
                
                s2 = datetime.datetime.now()
                USGS_POD_Utilities.logfilewrite(log, "Finished the Post Processing, process took {}.".format(str(s2-s1)), "High")            

                del newFolderName, newDirectory

                USGS_POD_Utilities.logfilewrite(log, "Updated the Complete Status in the Map Export Queue Table for map: " + str(map_name), "High")
                USGS_POD_Utilities.logfilewrite(log, "The final URL is: {}.".format(finalURL), "High")

                USGS_POD_Utilities.logfilewrite(log, "Export Complete", "High")

                exportEnd = time.time()
                #elapsedTime = (exportEnd - exportStart)
                totalElapsedTime = (exportEnd - t1)
                USGS_POD_Utilities.logfilewrite(log, " is end time ")
                USGS_POD_Utilities.logfilewrite(log, "Total Elapsed time is: " + str(totalElapsedTime))

                del exportEnd, exportStart, totalElapsedTime, exportStartDateTime

                parameters[1].value = file_name
                
                USGS_POD_Utilities.logfilewrite(log, "The requestor email is {}".format(userEmailAddress))
                if userEmailAddress is not None and "map_id" in product.keys() :
                    USGS_POD_Utilities.invoke_serverless_signer(file_name,product.exporter,str(product.map_id),str(product.process_id),'not_fail')
                    USGS_POD_Utilities.logfilewrite(log, "Email Sent")  
                    
                arcpy.CheckInExtension("foundation") 
                arcpy.CheckInExtension("Spatial")
                
                USGS_POD_Utilities.logfilewrite(log, "------------")
                endTimeStamp = datetime.datetime.now()
                USGS_POD_Utilities.logfilewrite(log, "The export is complete, total time is: {}.".format(str(endTimeStamp - exportStartTimeStamp)))                 
                return

        except arcpy.ExecuteError: 
            arcpy.AddError(arcpy.GetMessages())
            messages = 'The Map Export Process Failed. Error Message:  {}'.format(arcpy.GetMessages())
            file_name = None                
            USGS_POD_Utilities.logfilewrite(log, messages)
            sql = """ UPDATE """ + USGS_POD_Utilities.queue_table + """ SET (map_status, error_message) = (%s,%s) WHERE map_id = """ + "'" + product.map_id + "'"
            updateFields = (3, messages)
            USGS_POD_Utilities.updateFlatTable(sql, updateFields, log)
            del sql, updateFields

        except Exception as ex:
            arcpy.AddError(ex.message)
            tb = sys.exc_info()[2]
            tbinfo = traceback.format_tb(tb)[0]
            arcpy.AddError("Traceback info:\n" + tbinfo)
            messages = 'The Map Export Process Failed. Error Message:  {}'.format(ex)
            file_name = None
            USGS_POD_Utilities.logfilewrite(log, messages)
            sql = """ UPDATE """ + USGS_POD_Utilities.queue_table + """ SET (map_status, error_message) = (%s,%s) WHERE map_id = """ + "'" + product.map_id + "'"
            updateFields = (3, messages)
            USGS_POD_Utilities.updateFlatTable(sql, updateFields, log)
            del sql, updateFields

        finally:
            try:
                log.close()
            except:
                arcpy.AddMessage("The Map Generation log does not exist.")
            gc.collect()  


# For Debugging Python Toolbox Scripts
# comment out when running in ArcMap
#def main():
    #g = MapGeneratorAutoLabeling()
    #par = g.getParameterInfo()
    #g.execute(par, None)
#if __name__ == '__main__':
    #main()
