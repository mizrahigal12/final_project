from project_config import cnx 


def mysqlconnect():
    cur = cnx.cursor()
    cur.execute("select @@version")
    output = cur.fetchall()
    print(output)
        
        # To close the connection
    cnx.close()
    
# Driver Code
if __name__ == "__main__" :
    mysqlconnect()