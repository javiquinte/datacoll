import json

class DC_Module(object):
    def __init__(self, dc):
        # dc is supposed to be an instance of a DCApp class
        dc.registerAction(("features",), self.features)

        # We keep a copy of it
        self.__dc = dc

    def features(self, environ):
        """Read the features of the system and return them in JSON format.

        :returns: System capabilities in JSON format
        :rtype: string
        """

        syscapab = {"pidProviderType": "",
                    "enforcesAccess": False,
                    "supportsPagination": False,
                    "ruleBasedGeneration": False,
                    "maxExpansionDepth": 0}
        return json.dumps(syscapab)
