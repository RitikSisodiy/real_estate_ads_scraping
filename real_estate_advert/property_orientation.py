property_orientations = (
    ("NA","","Non précisé"),
    ("Nord", "North"),
    ("Nord-Est", "NordEst", "Nord/Est", "NordEst/Ouest", "NordOuest", "North-East"),
    ("Est", "East"),
    ("Sud-Est", "SudEst", "Sud/Est", "Est/Sud", "Est-Sud", "Nord/Est", "Sud/E", "Nord-Est(façade)", "SudEst-NordOuest", "Sud-Est/Nord-Ouest", "South-East"),
    ("Sud", "South"),
    ("Sud-Ouest", "SudOuest", "Sud/ouestsalon/séjo", "Sud-Ouestterrasse", "Sud(faï¿½ade)-Nor", "sudbaie", "sudest-nordouest", "South-West"),
    ("Ouest", "OUEST", "Ouest-Est", "ESTOUEST", "Est/ouest", "EST-OUEST", "Ouestterrasse", "Nord-Ouest/Sud-Est", "Nord-Ouest", "Nord/Ouest", "NordOuest", "Sud/Est", "Sud-Est/Nord-Ouest", "West"),
    ("Nord-Sud", "NordSud", "NORD/SUD", "traversantnord-sud", "NORDSUDESTOUEST", "NORDSUDOUEST", "NORDSUDESTOUEST", "SUD/NORD", "SUD-NORD", "North-South"),
    ("Est-Ouest", "est-ouest", "traversantest-ouest", "EST/SUD/OUEST", "Est", "Est-Sud-Ouest", "East-West")
)


# Loop through the orientation groups and check if the orientation is in each group
def getOrientation(orientation):
    for group in property_orientations:
        if orientation in group:
            orientation = group[0]
            return orientation
    return "NA"
# sample query to update existing record
# {
#     "query": {
#         "exists": {
#             "field": "exposure"
#         }
#     },
#     "script": {
#         "source": "for (def group: params.orientation_groups) { if (group.contains(ctx._source.exposure)) {ctx._source.exposure = group[0]; break;}}ctx._source.exposure = \"NA\";",
#         "params": {
#             "orientation_groups": [
#                 ["NA","","Non précisé"],
#                 ["Nord", "North"],
#                 ["Nord-Est", "NordEst", "Nord/Est", "NordEst/Ouest", "NordOuest", "North-East"],
#                 ["Est", "East"],
#                 ["Sud-Est", "SudEst", "Sud/Est", "Est/Sud", "Est-Sud", "Nord/Est", "Sud/E", "Nord-Est(façade)", "SudEst-NordOuest", "Sud-Est/Nord-Ouest", "South-East"],
#                 ["Sud", "South"],
#                 ["Sud-Ouest", "SudOuest", "Sud/ouestsalon/séjo", "Sud-Ouestterrasse", "Sud(faï¿½ade)-Nor", "sudbaie", "sudest-nordouest", "South-West"],
#                 ["Ouest", "OUEST", "Ouest-Est", "ESTOUEST", "Est/ouest", "EST-OUEST", "Ouestterrasse", "Nord-Ouest/Sud-Est", "Nord-Ouest", "Nord/Ouest", "NordOuest", "Sud/Est", "Sud-Est/Nord-Ouest", "West"],
#                 ["Nord-Sud", "NordSud", "NORD/SUD", "traversantnord-sud", "NORDSUDESTOUEST", "NORDSUDOUEST", "NORDSUDESTOUEST", "SUD/NORD", "SUD-NORD", "North-South"],
#                 ["Est-Ouest", "est-ouest", "traversantest-ouest", "EST/SUD/OUEST", "Est", "Est-Sud-Ouest", "East-West"]
#             ]
#         }
#     }
# }