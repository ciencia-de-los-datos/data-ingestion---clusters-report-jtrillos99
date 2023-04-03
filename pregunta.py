"""
Ingestión de datos - Reporte de clusteres
-----------------------------------------------------------------------------------------

Construya un dataframe de Pandas a partir del archivo 'clusters_report.txt', teniendo en
cuenta que los nombres de las columnas deben ser en minusculas, reemplazando los espacios
por guiones bajos; y que las palabras clave deben estar separadas por coma y con un solo 
espacio entre palabra y palabra.


"""
import pandas as pd


def ingest_data():
    
    df = pd.read_csv('clusters_report.txt', sep='\t', header=[0,1])
    col = df.columns
    df.drop([0], axis=0, inplace=True)
    df.columns = ["0"]
       
    
    for i in df:
        df[i] = df[i].str.replace('  ', ' ')
                     
    df2 = df["0"].str.split(' ', n=14, expand=True)
   
    df2.drop([0,1,3,6,7,8,9,10], axis=1, inplace=True)
    
    df2["2nd"]=df2[4] + df2[5]

    
    df2["3rd"]=df2[11] + df2[12] + df2[13]
    df2.drop([4,5,11,12,13], axis=1, inplace=True)
    df2=df2[[2,'2nd','3rd',14]]   

    df2["3rd"] = df2["3rd"].str.replace("%", "")
    df2["3rd"] = df2["3rd"].str.replace(",", ".")

    count = 1
    for i in df2[2]:
        try:
            df2[2][count] = int(i)
        except:
            df2[2][count] = 0       
        count += 1
    
    count = 1
    for i in df2[2]:
        if i != 0:
            a=i
        else: 
            df2[2][count] = a
        count=count+1 
    
    df2=df2.groupby(2).sum()
    
    df2.reset_index(drop=False, inplace=True)
    
    df2[14] = df2[14].str.replace(",", ", ")
    df2[14] = df2[14].str.replace("  ", " ")
    df2[14] = df2[14].str.replace("  ", " ")
    df2[14] = df2[14].str.replace("  ", " ")
    df2[14] = df2[14].str.replace("  ", " ")
    df2[14] = df2[14].str.replace(".", "",regex=False)
    df2[14] = df2[14].str.strip()

        
    df2.columns = ["cluster", "cantidad_de_palabras_clave", "porcentaje_de_palabras_clave", "principales_palabras_clave"]
    df2 = df2.astype({'cantidad_de_palabras_clave': 'int64', 'porcentaje_de_palabras_clave': 'float64'})
    
    
        
    return df2

#print(ingest_data().principales_palabras_clave.to_list()[0])

