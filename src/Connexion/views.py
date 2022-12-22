import pandas as pd
from django.shortcuts import redirect, render
from django.contrib.auth.models import User
from django.contrib import messages
from io import TextIOWrapper
from .models import *
from sqlalchemy import create_engine
from django.contrib.auth import authenticate, login, logout
from django.db import migrations, models, connection
# Create your views here.


def Graph(request):
    return render(request,"Authentification/Graph.html" )


def Home(request):
    if request.method == "POST":
        try:
            engine = create_engine('postgresql://postgres:0000@localhost/DataViz', echo=False)# Méthode de mysqlalchemy pour se connecter à la BDD
        except:
            print('Erreur de l_utilisation de l_engine')
        Data1 = request.FILES["Data0"]
        File = TextIOWrapper(Data1, newline="")
        pd.options.mode.chained_assignment = None 
        Data = pd.read_csv(File, encoding="Latin1", parse_dates=True)
        Data = Data.drop(['CustomerID'], axis=1) #Suppression de la colonne inutiles
        Data.rename(columns = {'Country':'identifiant_pays'}, inplace = True)
        Data.rename(columns = {'UnitPrice':'prix_unitaire'}, inplace = True)
        Data.rename(columns = {'InvoiceNo':'identifiant_facture'}, inplace = True)
        Data.rename(columns = {'Quantity':'volume'}, inplace = True)
        Data.rename(columns = {'StockCode':'identifiant_produit'}, inplace = True)
        Data.rename(columns = {'InvoiceDate':'date_facture'}, inplace = True)
        Data.rename(columns= {'Description': 'description'}, inplace=True)

        """
        Filtre des descriptions
        """
        indexdescription = Data[(Data['description']=='?')
        |(Data['description']=='? sold as sets?')
        |(Data['description']=='?missing')
        |(Data['description']=='??')
        |(Data['description']=='?display')
        |(Data['description']=='???lost')
        |(Data['description']=='??missing')
        |(Data['description']=='???missing')
        |(Data['description']=='????missing')
        |(Data['description']=='????damages????')
        |(Data['description']=='?lost')
        |(Data['description']=='?lost')
        |(Data['description']=='Adjustment')
        |(Data['description']=='adjustment')
        |(Data['description']=='rcvd be air temp fix for dotcom sit')
        |(Data['description']=='returned')
        |(Data['description']=='mailout')
        |(Data['description']=='on cargo order')
        |(Data['description']=='Next Day Carriage')
        |(Data['description']=='found')
        |(Data['description']=='alan hodge cant mamage this section')
        |(Data['description']=='Found')
        |(Data['description']=='dotcom')
        |(Data['description']=='Dotcomgiftshop Gift Voucher £100.00')
        |(Data['description']=='Adjust bad debt')
        |(Data['description']=='amazon')
        |(Data['description']=='test')
        |(Data['description']=='taig adjust')
        |(Data['description']=='allocate stock for dotcom orders ta')
        |(Data['description']=='add stock to allocate online orders')
        |(Data['description']=='for online retail orders')
        |(Data['description']=='Amazon')
        |(Data['description']=='found box')
        |(Data['description']=='damaged')
        |(Data['description']=='Found in w/hse')
        |(Data['description']=='michel oops')
        |(Data['description']=='wrongly coded 20713')
        |(Data['description']=='Had been put aside.')
        |(Data['description']=='Sale error')
        |(Data['description']=='Amazon Adjustment')
        |(Data['description']=='wrongly marked 23343')
        |(Data['description']=='Marked as 23343')
        |(Data['description']=='wrongly coded 23343')
        |(Data['description']=='Found by jackie')
        |(Data['description']=='check')
        |(Data['description']=='wrongly marked')
        |(Data['description']=='amazon adjust')
        |(Data['description']=='dotcomstock')
        |(Data['description']=='John Lewis')
        |(Data['description']=='check?')
        |(Data['description']=='AMAZON')
        |(Data['description']=='amazon sales')
        |(Data['description']=='wrongly sold (22719) barcode')
        |(Data['description']=='rcvd be air temp fix for dotcom sit')
        |(Data['description']=='did  a credit  and did not tick ret')
        |(Data['description']=='returned')
        |(Data['description']=='test')
        |(Data['description']=='taig adjust')
        |(Data['description']=='Manual')
        |(Data['description']=='')
        |(Data['description']=='on cargo order')
        |(Data['description']=='incorrectly credited C550456 see 47')
        |(Data['description']=='FOUND')].index
        Data.drop(indexdescription, inplace=True)

        """
        Filtre concernant l'identifiant_produit
        """
        Data.drop_duplicates(subset=['identifiant_produit'],inplace=True)
        indexidentifiant_produit = Data[(Data['identifiant_produit']=='C2')
        |(Data['identifiant_produit']=='DOT')
        |(Data['identifiant_produit']=='M')
        |(Data['identifiant_produit']=='POST')
        |(Data['identifiant_produit']=='BANK CHARGES')
        |(Data['identifiant_produit']=='AMAZONFEE')
        |(Data['identifiant_produit']=='D')
        |(Data['identifiant_produit']=='B')
        |(Data['identifiant_produit']=='gift_0001_10')
        |(Data['identifiant_produit']=='gift_0001_20')
        |(Data['identifiant_produit']=='gift_0001_30')
        |(Data['identifiant_produit']=='gift_0001_40')
        |(Data['identifiant_produit']=='gift_0001_50')
        |(Data['identifiant_produit']=='S')
        |(Data['identifiant_produit']=='PADS')
        |(Data['identifiant_produit']=='CRUK')].index
        Data.drop(indexidentifiant_produit, inplace=True)

        """
        Filtres plus spécifiques
        """
        Data.drop_duplicates(subset=['identifiant_facture'], inplace=True)
        indexCountry = Data[(Data['identifiant_pays']=='Unspecified')].index
        Data.drop(indexCountry, inplace=True)

        """
        Tout les Volumes (quantités), inférieurs ou égaux à 0 sont supprimés
        """
        indexvolume = Data[(Data['volume']<=0)].index
        Data.drop(indexvolume, inplace=True)

        print(Data.head())
        print(Data.shape)

        IDpays = Data[['identifiant_pays']]
        IDpays.drop_duplicates(inplace=True)

        with connection.cursor() as c:
            remove_constraint = """
                ALTER TABLE data_management_facture
                    DROP CONSTRAINT data_management_fact_idregion_id_33c2896f_fk_data_mana;
            """
            create_temporary_table = """
                CREATE TABLE IF NOT EXISTS temp_table AS SELECT * FROM pays;
                
            """
            drop_temporary_table = """
                DROP TABLE IF EXISTS temp_table;
            """
            add_new_values = """
                INSERT INTO pays (identifiant_pays)
                    SELECT identifiant_pays FROM temp_table
                    ON CONFLICT (identifiant_pays) DO NOTHING
            """
            add_constraint = """
                ALTER TABLE data_management_facture
                    ADD CONSTRAINT data_management_fact_idregion_id_33c2896f_fk_data_mana
                        FOREIGN KEY (idregion_id)
                        REFERENCES data_management_region
                            (idregion)
                        ON DELETE NO ACTION ON UPDATE NO ACTION;
            """
            # c.execute(remove_constraint)
            c.execute(create_temporary_table)
            IDpays.to_sql("temp_table", con=engine, index=False, if_exists='append')
            c.execute(add_new_values)
            # c.execute(add_constraint)
            c.execute(drop_temporary_table)



        # IDpays.to_sql(
        #         name='pays',
        #         con=engine,
        #         if_exists='append',
        #         index=False,
        #     )
        # try:
        #     Lepays.to_sql(
        #         name='pays',
        #         con=engine,
        #         if_exists='append',
        #         index=False,
        #     )
        # except:
        #     print('Erreur import pays')
            
        # row_iter0 = Lepays.iterrows()
        # objs1 =[
        #   Pays (
        #     pays = row ['pays'],
        #   )
        #   for index, row in row_iter0
        #        ]
        # Pays.objects.bulk_create(objs1)


        """Exportation d'identifiant_produit, de la description, du prix_unitaire et de identifiant_pays vers la table identifiant_produit de la BDD"""
        IDproduit = Data[['identifiant_produit', 'description', 'prix_unitaire', 'identifiant_pays']].copy()


        with connection.cursor() as c:
            remove_constraint = """
                ALTER TABLE identifiant_produit
                    DROP CONSTRAINT identifiant_produit_identifiant_pays_fkey;
            """
            create_temporary_table = """
                CREATE TABLE IF NOT EXISTS temp_table AS SELECT * FROM identifiant_produit;
                
            """
            drop_temporary_table = """
                DROP TABLE IF EXISTS temp_table;
            """
            add_new_values = """
                INSERT INTO identifiant_produit (identifiant_produit, description, prix_unitaire, identifiant_pays)
                    SELECT identifiant_produit, description, prix_unitaire, identifiant_pays FROM temp_table
                    ON CONFLICT (identifiant_produit) DO NOTHING
            """
            add_constraint = """
                ALTER TABLE identifiant_produit
                    ADD CONSTRAINT identifiant_produit_identifiant_pays_fkey
                        FOREIGN KEY (identifiant_pays)
                        REFERENCES pays
                            (identifiant_pays)
                        ON DELETE NO ACTION ON UPDATE NO ACTION;
            """
            c.execute(remove_constraint)
            c.execute(create_temporary_table)
            IDproduit.to_sql("temp_table", con=engine, index=False, if_exists='append')
            c.execute(add_new_values)
            c.execute(add_constraint)
            c.execute(drop_temporary_table)

        # IDproduit.to_sql(
        #         name='identifiant_produit',
        #         con=engine,
        #         if_exists='append',
        #         index=False,
        #     )
        # try:
        #     Leproduit.to_sql(
        #         name='produit',
        #         con=engine,
        #         if_exists='append',
        #         index=False,
        #     )
        # except: print('Erreur import produit')
        # row_iter = Leproduit.iterrows()
        # objs =[
        #   Produit (
        #     stock_code = row ['stock_code'],
        #     description = row ['description'],
        #   )
        #   for index, row in row_iter
        #        ]
        # Produit.objects.bulk_create(objs)
                


        """Exportation de identifiant_facture, de la date_facture et de identifiant_pays vers la table identifiant_facture"""
        IDfacture = Data[['identifiant_facture', 'date_facture', 'identifiant_pays']].copy()
        # nfacture.drop_duplicates(subset=['n_facture'], inplace=True)
        IDfacture['date_facture']= pd.to_datetime(IDfacture['date_facture'])
        IDfacture.to_sql(
                name='identifiant_facture',
                con=engine,
                if_exists='append',
                index=False,
            )
        # try:
        #     nfacture.to_sql(
        #         name='n_facture',
        #         con=engine,
        #         if_exists='append',
        #         index=False,
        #     )
        # except: print('Erreur import n_facture')


        """Exportation vers la Table, information_commande"""
        détail_commande = Data[['identifiant_facture', 'identifiant_produit', 'volume']].copy()
        détail_commande.to_sql (
                name="information_commande",
                con=engine,
                if_exists='append',
                index=False,
            )
        # try:
        #     détail_commande.to_sql (
        #         name="détail_commande",
        #         con=engine,
        #         if_exists='append',
        #         index=False,
        #     )
        # except: 
        #     print('Erreur import détail commande')
        #     print(détail_commande)
        # infos1 = Data.head()
        # description = Data.describe()
        context = {
            "N1": IDpays,
            "N2": IDproduit,
            "N3": IDfacture,
            "N4": indexvolume,
            "N5": détail_commande,
        }
    return render(request, "Authentification/index.html", {})


def Login(request):
    if request.method == "POST":
        Username = request.POST["Username"]#Ici on prend les names="", données dans les pages HTML
        Mot_de_passe = request.POST["Mot_de_passe"]
        user = authenticate(username=Username, password=Mot_de_passe)

        if user is not None:
            login(request, user)
            # firstname = user.first_name #First_name est une propriété de l'objet user, registré dans la variable firstname
            # return redirect("Home", {"firstname":firstname})
            return redirect("Home")
        else:
            messages.error(request, "Un problème est survenu lors de l'authentification")
            return redirect("Login")
    else:   
        return render(request, "Authentification/Login.html")


def Logout(request):
    logout(request)
    messages.success(request, "Vous avez été déconnecté avec succès")
    return redirect("Login")


def Importation(request):
    return render(request, "Graph")






# def Register(request): #Je vais récupérer ce qui a été entré par l'utilisateur
#     if request.method == "POST":
#         Username = request.POST["Username"]#Ici on prend les names="", données dans les pages HTML
#         firstname = request.POST["firstname"]
#         Lastname = request.POST["Lastname"]
#         E_Mail = request.POST["E_Mail"]
#         Mot_de_passe = request.POST["Mot_de_passe"]
#         Confirmation_MDP = request.POST["Confirmation"]
#         if User.objects.filter(username= Username):
#             messages.error(request, "Ce nom d'utilisateur(trice) existe déjà")
#             return redirect("Register")
#         if User.objects.filter(email=E_Mail):
#             messages.error(request,"Cet addresse E-Mail a déjà été utilisé")
#             return redirect("Register")
#         if not Username.isalnum(): #si Username n'est pas composé de caractères alpha-numériques, alors on reste sur la page Register
#             messages.error(request, "Le nom d'utilisateur doit contenir des chiffres et des caractères alphabétiques")
#             return redirect("Register")
#         if Mot_de_passe != Confirmation_MDP:
#             """"
#             Si le mot de passe est différent de la confirmation du mot de passse,
#             j'affiche un message d'erreur et on reste sur cette page
#             """
#             messages.error(request, "Les mots de passe de concordent pas, veuillez ré-essayer")
#             return redirect("Register")
        
#         Mon_Utilisateur = User.objects.create_user(Username, E_Mail, Mot_de_passe)
#         Mon_Utilisateur.first_name = firstname
#         Mon_Utilisateur.last_name = Lastname
#         Mon_Utilisateur.save() #J'enregistre ces informations dans la table User
#         messages.success(request, "Votre compte vient d'être créé avec succès")
#         return redirect("Login")

#     return render(request, "Authentification/Register.html")

    