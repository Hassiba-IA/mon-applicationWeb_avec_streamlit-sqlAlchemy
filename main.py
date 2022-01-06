### Etudiants classe IA-M2-VS-DA1

# Mvogo Djuissi Annabelle Audrey
# Martinez Beatriz
# Zerouklame Hassiba

# Avant l'execution du script il faudrait avoir installé les pachages définit ci dessous

import streamlit as st
from matplotlib import pyplot 
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
from PIL import Image
import datetime
import altair as alt

#modification taille de la page
st.set_page_config(layout="wide")

st.title('Analyse des Données Financières de Ventes')
st.write("\n\n")

# Chargement du  dataframe des données financières // NB: le chemin d'accès au fichier et au lien de l'image sont à modifier selon l'environnement

df=pd.read_csv('C:\\Users\\audrey.mvogo\\Cours_Master_2\\Cours_Python_Flask_Streamit\\Devoir_Analyse_Data\\Donnee_financiaire.csv',header=0,delimiter=';')


#je charge l'image de la base de données crées
image = Image.open('C:\\Users\\audrey.mvogo\\Cours_Master_2\\Cours_Python_Flask_Streamit\\Devoir_Analyse_Data\\bd.PNG')

   
    
# construction des dataframes produit, segment, pays, ventes

produit=df[["produit","cout_fabrication"]].drop_duplicates()


# corecction des index et ajout de la colonne Id

produit.index=range(1,produit.shape[0]+1 ) # ajout de l'index de façon incrementiel (on met +1 car produit commence à index 0)
produit['id_produit']=produit.index
produit.rename(columns={"produit": "nom_produit"}, inplace=True)


segment=df[["segment"]].drop_duplicates() 
segment.index=range(1,segment.shape[0]+1 ) 
segment['id_segment']=segment.index
segment.rename(columns={"segment": "nom_segment"}, inplace=True)


pays=df[["pays"]].drop_duplicates() 
pays.index=range(1,pays.shape[0]+1 ) 
pays['id_pays']=pays.index
pays.rename(columns={"pays": "nom_pays"}, inplace=True)


#creation des dictionnaire pour le mapping des trois table
p=dict(zip(produit['nom_produit'],produit['id_produit']))
s=dict(zip(segment['nom_segment'],segment['id_segment']))
pa=dict(zip(pays['nom_pays'],pays['id_pays']))


ventes=df[["segment","pays","produit","remise","nombre_vendu","prix_vente","ventes_brutes","ventes","cout_ventes","profit","date","nb_mois","mois","annee"]]

# renommage de colonnes

ventes.rename(columns={"segment": "id_segment", "pays": "id_pays", "produit": "id_produit"}, inplace=True)

#mapping des valeurs

ventes["id_produit"]=ventes["id_produit"].map(p)
ventes["id_segment"]=ventes["id_segment"].map(s)
ventes["id_pays"]=ventes["id_pays"].map(pa)


# ajout de la colonne ID_vente de la table  ventes et ajout de la colonne Id_ventes et mapping des trois colonne
ventes.index=range(1,ventes.shape[0]+1 ) 
ventes['id_vente']=ventes.index

# affichage de la table vente
#ventes


# pour la connection à la base de données il est necessaire d'installer en local le serveur Wamp ou Mamp qui va herbergé la base de donnée
# connection à la base de données hébergé sur le serveur mysql (en localhost)

engine = create_engine("mysql+pymysql://root:@localhost/finance")

# creation de la base de données si inéxistante

if not database_exists(engine.url):
    create_database(engine.url)
else:
    engine.connect() # connection à la base de données si elle existe

connection = engine.raw_connection()

#creation des tables 

#produit

sql_table1=""" 
               CREATE TABLE IF NOT EXISTS produit (
                    id_produit INT NOT NULL,
                    nom_produit VARCHAR(40)NOT NULL,
                    cout_fabrication INT NOT NULL,
                    PRIMARY KEY(id_produit)
                ); """

try:
    engine.execute(sql_table1)
except Exception as e:
    st.write('Message d\'erreur : \n{}'.format(e))
        
# segment

sql_table2="""
               CREATE TABLE IF NOT EXISTS segment (
                    id_segment INT NOT NULL,
                    nom_segment VARCHAR(40)NOT NULL,
                    PRIMARY KEY(id_segment)
                ); """


try:
    engine.execute(sql_table2)
except Exception as e:
    st.write('Message d\'erreur : \n{}'.format(e))
    
# pays

sql_table3=""" 
              CREATE TABLE IF NOT EXISTS pays (
                    id_pays INT NOT NULL,
                    nom_pays VARCHAR(40)NOT NULL,
                    PRIMARY KEY(id_pays)
                ); """

try:
    engine.execute(sql_table3)
except Exception as e:
    st.write('Message d\'erreur : \n{}'.format(e))

#ventes

sql_table4="""
              CREATE TABLE IF NOT EXISTS ventes (
                    id_vente INT NOT NULL,
                    id_segment INT NOT NULL,
                    id_pays INT NOT NULL,
                    id_produit INT NOT NULL,
                    remise varchar(20) NOT NULL,
                    nb_vente INT NOT NULL,
                    prix_vente INT NOT NULL,
                    ventes_brutes INT NOT NULL,
                    ventes INT NOT NULL,
                    cout_ventes INT NOT NULL,
                    profit INT NOT NULL,
                    date   DATE NOT NULL,
                    num_mois INT(2) NOT NULL,
                    mois varchar(12) NOT NULL,
                    annee int(4) NOT NULL,
                    PRIMARY KEY(id_vente),
                    FOREIGN KEY(id_produit) REFERENCES produit (id_produit),
                    FOREIGN KEY(id_pays) REFERENCES pays (id_pays),
                    FOREIGN KEY(id_segment) REFERENCES segment (id_segment)
                    
                ); """


try:
    engine.execute(sql_table4)
except Exception as e:
    st.write('Message d\'erreur : \n{}'.format(e))



# insertion des données dans les tables

#table segment
cur = connection.cursor() # sous element de la connection qui permet d'effectuer les opérations itératives 
cur.execute("delete from ventes") # je supprime premièrement les données de la table fille
cur.execute("delete from segment")
for index, row in segment.iterrows():
  
    try:
        cur.execute("INSERT INTO segment (id_segment,nom_segment) VALUES ('{}','{}')".format(row["id_segment"],row["nom_segment"]))
    # S'il y a une erreur je l'affiche et ensuite j'affiche la ligne qui n'a pas pu être insérée
    except Exception as e:
        st.write(e)
        st.write('erreur d insertion',(row["id_segment"],row["nom_segment"]))
        
connection.commit()
cur.close()

#table pays
cur = connection.cursor() 


cur.execute("delete from pays")
for index, row in pays.iterrows():
    try:
        cur.execute("INSERT INTO pays (id_pays,nom_pays) VALUES ('{}','{}')".format(row["id_pays"],row["nom_pays"]))
    # S'il y a une erreur je l'affiche et ensuite j'affiche la ligne qui n'a pas pu être insérée
    except Exception as e:
        st.write(e)
        st.write('erreur d insertion',(row["id_pays"],row["nom_pays"]))
        
connection.commit()
cur.close()


#table produit
cur = connection.cursor() 
cur.execute("delete from produit")
for index, row in produit.iterrows():
    try:
        cur.execute("""INSERT INTO produit (id_produit,nom_produit,cout_fabrication) VALUES ('{}','{}','{}')""".format(row["id_produit"],row["nom_produit"],row["cout_fabrication"]))
    # S'il y a une erreur je l'affiche et ensuite j'affiche la ligne qui n'a pas pu être insérée
    except Exception as e:
        st.write(e)
        st.write('erreur d insertion',(row["id_produit"],row["nom_produit"]))
        
connection.commit()
cur.close()

#table ventes
cur = connection.cursor() 

for index, row in ventes.iterrows():
    try:
        cur.execute("""INSERT INTO ventes (id_vente,id_segment,id_pays,id_produit,remise,nb_vente,prix_vente,
        ventes_brutes,ventes,cout_ventes,profit,date,num_mois,mois,annee) VALUES   ('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}',STR_TO_DATE('{}','%d/%m/%Y'),'{}','{}','{}')""".format(row["id_vente"],
        row["id_segment"],row["id_pays"],row["id_produit"],row["remise"],row["nombre_vendu"],
        row["prix_vente"],row["ventes_brutes"],row["ventes"],row["cout_ventes"],row["profit"],row["date"],
        row["nb_mois"],row["mois"],row["annee"]))
    # S'il y a une erreur je l'affiche et ensuite j'affiche la ligne qui n'a pas pu être insérée
    except Exception as e:
        st.write(e)
        st.write('erreur d insertion',(row["id_vente"],
        row["id_segment"],row["id_pays"],row["id_produit"],row["remise"],row["nombre_vendu"],
        row["prix_vente"],row["ventes_brutes"],row["ventes"],row["cout_ventes"],row["profit"],row["date"],
        row["nb_mois"],row["mois"],row["annee"]))
        
connection.commit()
cur.close()



#Affichage des éléments de la page de façon structurées 


st.header('Données sources') 
df
st.header('Base de données') 
st.image(image,width=700)

#

st.header('Suivi des ventes brutes par Produit et Segment') 

col1, col2 = st.columns([2,4])

with col1:
    var_pays = st.selectbox('Choisissez le Pays',('Canada', 'France', 'Germany','Mexico','United States of America'))
    st.write('\n')  
    var_date_deb = st.date_input(label='choisissez la date de debut',value=datetime.date(2013, 1, 1) ,min_value= datetime.date(2013, 1, 1), max_value=datetime.date(2014, 12, 1))
    st.write('\n') 
    var_date_fin = st.date_input(label='choisissez la date de fin', value=datetime.date(2014, 12, 1), min_value= datetime.date(2013, 1, 1), max_value=datetime.date(2014, 12, 1))
    

with col2:

#selection des données depuis la base de données
    
    data_anal_vente_brute=pd.read_sql("""Select p.nom_produit produit,s.nom_segment segment,sum(ventes_brutes) ventes_brutes from ventes v 
inner join produit p on p.id_produit = v.id_produit
inner join segment s on s.id_segment = v.id_segment
inner join pays pa on pa.id_pays = v.id_pays
where v.date between '{}' and '{}' and pa.nom_pays = '{}'
group by p.nom_produit,s.nom_segment""".format(var_date_deb.strftime("%Y-%m-%d"),
var_date_fin.strftime("%Y-%m-%d"),var_pays),connection,parse_dates=["date"])
    
    chart1 = (
    alt.Chart(data_anal_vente_brute)
    .mark_bar()
    .encode(x="produit", y="ventes_brutes", color="produit", column="segment"))
    st.write(chart1)
    
    

st.header('Suivi du Profit d\'un Produit par Pays sur une Année') 

col1, col2 = st.columns([2,4])


with col1:
    
    var_annee = st.selectbox('Choisissez l\'année',('2013', '2014'))
    var_produit = st.selectbox('Choisissez le produit',('Carretera', 'Montana', 'Paseo','Velo','VTT','Amarilla'))
    


with col2:
    data_anal_profit_global=pd.read_sql("""Select pa.nom_pays Pays,v.mois mois,v.num_mois,sum(profit) Profit from ventes v 
inner join pays pa on pa.id_pays = v.id_pays
inner join produit p on p.id_produit = v.id_produit
where p.nom_produit like '%{}%' and v.annee='{}' group by pa.nom_pays,v.mois
order by v.num_mois asc """.format(var_produit,var_annee),connection)
    
    chart2 = alt.Chart(data_anal_profit_global).mark_line().encode(
    x=alt.X('mois'),
    y=alt.Y('Profit'),
    color=alt.Color("Pays")
    )
    st.altair_chart(chart2, use_container_width=True)
     

engine.dispose()    
    
 
   
    
    







    

 
























