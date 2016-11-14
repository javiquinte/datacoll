import json

class DC_Module(object):
    def __init__(self, dc):
        dc.registerAction("/features", self.features)

        # We keep a copy of it
        self.__dc = dc

    def features(self):
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
