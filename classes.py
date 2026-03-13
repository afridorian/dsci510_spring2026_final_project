class Player:

    nameId = {} #key is player ID, name. value is list of seasons played (use as quick reference index of all players, only store professional players in global)

    def __init__(self,id,name,position,teams:tuple,activeYears:int,seasons:dict,stats:dict,awards):
        self.id = id
        self.name = name
        self.position = position
        self.team = teams #professional and collegiate teams
        self.activeYears = activeYears
        self.seasons = seasons #dictionary of season averages. key is season, value is dict of season average stats
        self.stats = stats #dictionary of lifetime average stats
        self.awards = awards

class PointGuard(Player):

    def__init__(self,awards)
    pass

class ShootingGuard(Player):
    pass

class Forward(Player):
    pass

class PowerForward(Player):
    pass

class Center(Player):