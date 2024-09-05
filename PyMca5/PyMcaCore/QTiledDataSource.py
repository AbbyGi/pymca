import logging
import numpy as np
import time
import sys

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
        self.refresh()

    def refresh(self):
        # ddict = {

        # }
        # self.sigUpdated.emit(self.sourceName)
        pass

    def getDataObject(self, key_list, selection=None, poll=False):
        if type(key_list) not in [type([])]:
            nolist = True
            key_list = [key_list]
        else:
            output = []
            nolist = False
        data = self.get_data_object(key_list, selection=selection)

        return data

    def _set_key(self, selection=None):
        """Sets key once a scan has been selected in Tiled Browser."""
        
        key = {
            "scan": selection.metadata['start']['uid'],
            "scan_id": selection.metadata['start']['scan_id'],
            "streams": list(selection),
            "selection": selection, 
        }

        return key
    
    def _set_data_channel_selection(self):
        """Retrieve Data Channel Selections from Tiled Data Channel Table."""
        _logger.debug("-------- QTiledDataSource _set_data_channel_selection")
        channel_sel = TiledDataChannelTable.getChannelSelection()
        self.chan_sel = {
            'x': channel_sel['x'],
            'y': channel_sel['y'],
            'm': channel_sel['m'],
            'Channel List': channel_sel['Data Channel List'],
        }
    
    def _get_key_info(self, selection):
        """Retrives key info."""

        key = self._set_key(selection=selection)
        key_info = {
            "SourceType": SOURCE_TYPE,
            "selection": selection,
            "key": key,
        }

        return key_info
    
    def get_data_object(self, key, selection=None):
        """Generate a dataObject that will be used to plot scan data."""
        _logger.debug("-------- QTiledDataSource get_data_object")
        dataObject = DataObject.DataObject()
        dataObject.info = self._get_key_info(selection)
        dataObject.data = key['selection']

        chan_sel = self.chan_sel

        # What data.info attributes to add?
        dataObject.info['selection'] = selection
        
        # data = [key['selection']][datachannel][data]
        # If data channel selected in x axis go to data and time
        dataObject.data.x = dataObject.data.chan_sel['x']['data']['time']
        # If data channel selected in y axis plot everything
        dataObject.data.y = dataObject.data.chan_sel['y']['data']
        # If data selected in m divide y by m and plot
        dataObject.data.m = dataObject.data.chan_sel['m']['data']

        return dataObject

    def isUpdated(self,key):
        pass
    
    
def _is_Tiled_Source(filename):
    try:
        if hasattr(filename, self.node_path):
            return True
    except Exception:
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
        