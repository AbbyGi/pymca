import copy
import logging
import numpy as np
import time
import sys
from functools import reduce
from typing import Union

from tiled.client import from_uri
from tiled.client.array import DaskArrayClient

from PyMca5.PyMcaCore import DataObject
from PyMca5.PyMcaGui.io.TiledDataChannelTable import TiledDataChannelTable
from PyMca5.PyMcaGui.io.QTiledWidget import TiledBrowser
from PyMca5.PyMcaGui.pymca import QSource


_logger = logging.getLogger(__name__)


SOURCE_TYPE = 'Tiled'

class QTiledDataSource(QSource.QSource):
    """
    Creates instance of a Tiled Data Source class. This is neccesary
    to create a Tiled Tab in the QDispatcher, which houses the Tiled
    Browser.

    This is largely based on the NexusDataSource class, but all Data 
    Source tabs (Spec, EDF, SPS) have an analogous class.

    See QDataSource.py
    """

    def __init__(self, nameInput):
        _logger.debug("-------- QTiledDataSource init")
        super().__init__()
        if isinstance(nameInput, list):
            nameList = nameInput
        else:
            nameList = [nameInput]
        self.sourceName = nameList
        self.sourceType = SOURCE_TYPE
        self.__sourceNameList = self.sourceName
        self.root = from_uri(self.sourceName[0])
        self.refresh()

    def refresh(self):
        # ddict = {

        # }
        # self.sigUpdated.emit(self.sourceName)
        pass

    def getDataObject(self, key_list, selection=None):
        """Retrieve a dataObject that will be used to plot scan data."""
        if not isinstance(key_list, list):
            key_list = [key_list]
        data = self.get_data_object(key_list, selection=selection)

        return data

    def get_data_object(self, key, selection=None):
        """Generate a dataObject that will be used to plot scan data."""
        _logger.debug("-------- QTiledDataSource get_data_object")
        _logger.debug(f'{key = }')
        _logger.debug(f'{selection = }')
        dataObject = DataObject.DataObject()
        dataObject.info = {
            "selectiontype": "1D",
            "selection": copy.deepcopy(selection),
            "LabelNames": selection["Channel List"].copy(),
        }
        dataObject.data = None
        channel_names = selection["Channel List"]
        # For now, only support one key (corresponding to one source)
        key = key[0]
        dataObject.x = (
            self._get_data(path=key, data_key=channel_names[channel_index])
            for channel_index in selection["x"]
        )
        dataObject.y = (
            self._get_data(path=key, data_key=channel_names[channel_index])
            for channel_index in selection["y"]
        )
        dataObject.m = (
            self._get_data(path=key, data_key=channel_names[channel_index])
            for channel_index in selection["m"]
        )

        # Limit to 1D data for now
        dataObject.x = tuple(
            self._ensure_max_dims(data, max_dims=1)
            for data in dataObject.x
        )
        dataObject.y = tuple(
            self._ensure_max_dims(data, max_dims=1)
            for data in dataObject.y
        )
        dataObject.m = tuple(
            self._ensure_max_dims(data, max_dims=1)
            for data in dataObject.m
        )

        return dataObject

    def _get_data(
        self,
        path: tuple[str],
        data_key: str,
        *,
        stream: str = "primary",
    ) -> DaskArrayClient:
        """Get array data from Tiled client"""
        return self.root[(*path, stream, "data", data_key)]

    def _ensure_max_dims(
        self,
        data: DaskArrayClient,
        *,
        max_dims: int = 1,
    ) -> Union[DaskArrayClient, tuple]:
        """Return data array if dimensions are no greater than max_dims.
        
            Otherwise return an null tuple with shape == max_dims
        """
        if len(data.shape) <= max_dims:
            return data
        else:
            null_tuple = reduce(lambda x, y: tuple((x,)), range(max_dims), ())
            return null_tuple

    def isUpdated(self,key):
        pass
    
    
def _is_Tiled_Source(sourceName):
    try:
        if (
            sourceName.startswith("tiled:")
            or sourceName.startswith("http://")
            or sourceName.startswith("https://")
        ):
            return True
    except Exception:
        pass
    
    return False
    
source_types = {SOURCE_TYPE: QTiledDataSource}

def DataSource(name="", source_type=SOURCE_TYPE):
  try:
     sourceClass = source_types[source_type]
  except KeyError:
     #ERROR invalid source type
     raise TypeError("Invalid Source Type, source type should be one of %s" %\
                     source_types.keys())
  return sourceClass(name)


if __name__ == "__main__":
    try:
        sourcename = sys.argv[1]
        key = sys.argv[2]
    except Exception:
        _logger.debug("Usage: QTiledDataSource <file> <key>")
        sys.exit()
    #one can use this:
    obj = QTiledDataSource(sourcename)
    #or this:
    obj = DataSource(sourcename)
    #data = obj.getData(key,selection={'pos':(10,10),'size':(40,40)})
    #data = obj.getDataObject(key,selection={'pos':None,'size':None})
    t0 = time.time()
    data = obj.getDataObject(key,selection=None)
    _logger.debug("elapsed = ",time.time() - t0)
    _logger.debug("info = ",data.info)
    if data.data is not None:
        _logger.debug("data shape = ",data.data.shape)
        _logger.debug(np.ravel(data.data)[0:10])
    else:
        _logger.debug(data.y[0].shape)
        _logger.debug(np.ravel(data.y[0])[0:10])
    data = obj.getDataObject('1.1',selection=None)
    r = int(key.split('.')[-1])
    _logger.debug(" data[%d,0:10] = " % (r-1),data.data[r-1   ,0:10])
    _logger.debug(" data[0:10,%d] = " % (r-1),data.data[0:10, r-1])
        