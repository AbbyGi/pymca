"""

A Stack plugin is a module that will be automatically added to the PyMca stack windows
in order to perform user defined operations on the data stack.

These plugins will be compatible with any stack window that provides the functions:
    #data related
    getStackDataObject
    getStackData
    getStackInfo
    setStack

    #images related
    addImage
    removeImage
    replaceImage

    #mask related
    setSelectionMask
    getSelectionMask

    #displayed curves
    getActiveCurve
    getGraphXLimits
    getGraphYLimits

    #information method
    stackUpdated
    selectionMaskUpdated
"""
try:
    import StackPluginBase
except ImportError:
    from . import StackPluginBase
try:
    from PyMca import StackSimpleFitWindow
    import PyMca.PyMca_Icons as PyMca_Icons
except ImportError:
    print("FitStackPlugin importing from somewhere else")
    import StackSimpleFitWindow
    import PyMca_Icons

DEBUG = 0

class FitStackPlugin(StackPluginBase.StackPluginBase):
    def __init__(self, stackWindow, **kw):
        StackPluginBase.DEBUG = DEBUG
        StackPluginBase.StackPluginBase.__init__(self, stackWindow, **kw)
        self.methodDict = {}
        function = self.fitStack
        info = "Fit stack with user defined functions"
        icon = PyMca_Icons.fit
        self.methodDict["Fit Stack"] =[function,
                                       info,
                                       icon]
        self.__methodKeys = ["Fit Stack"]
        self.simpleFitWindow = None
                                     
    def stackUpdated(self):
        if self.simpleFitWindow is None:
            return
        self.__updateOwnData()

    #Methods implemented by the plugin
    def getMethods(self):
        return self.__methodKeys

    def getMethodToolTip(self, name):
        return self.methodDict[name][1]

    def getMethodPixmap(self, name):
        return self.methodDict[name][2]

    def applyMethod(self, name):
        return self.methodDict[name][0]()

    def __updateOwnData(self):
        activeCurve = self.getActiveCurve()
        if activeCurve is None:
            return
        #this can be problematic if a fit is going on...
        x, spectrum, legend, info = activeCurve
        xlabel = info['xlabel']
        ylabel = info['ylabel']
        xmin, xmax = self.getGraphXLimits()
        ymin, ymax = self.getGraphYLimits()
        mcaIndex = self.getStackInfo()['McaIndex']
        self.simpleFitWindow.setSpectrum(x,
                                         spectrum,
                                         xmin=xmin,
                                         xmax=xmax)
        self.simpleFitWindow.setData(x,
                                     self.getStackData(),
                                     data_index=mcaIndex)

    def fitStack(self):
        if self.simpleFitWindow is None:
            self.simpleFitWindow = StackSimpleFitWindow.StackSimpleFitWindow()
        self.__updateOwnData()
        self.simpleFitWindow.show()

MENU_TEXT = "Stack Simple Fitting"
def getStackPluginInstance(stackWindow, **kw):
    ob = FitStackPlugin(stackWindow)
    return ob
