import sqlite3

class tempDb:
    def __init__(self) -> None:
        self.con = sqlite3.connect("tutorial.db")
        self.cur = self.con.cursor()
        try:self.cur.execute("CREATE TABLE temp(id, data)")
        except:pass

    def get(self,id):
        res = self.cur.execute("""select * from temp where id=?""",(id,))
        d = res.fetchall()
        if (d):
            return d[0]
        else: return None
    def create(self,id,data):
        self.cur.execute("""INSERT INTO temp 
               VALUES (?,?);""", (id, data))
        self.con.commit()
        return True
    def getall(self):
        res = self.cur.execute('select * from temp')
        res = res.fetchall()
        return res
    def get_or_create(self,id,data=""):
        d = self.get(id)
        if d:
            return d
        else: self.create(id,data)
        return self.get(id)