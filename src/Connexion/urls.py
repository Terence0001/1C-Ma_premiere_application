from django.urls import path
from Connexion import views


urlpatterns = [
    path("", views.Home, name="Home"),
    # path("Register", views.Register, name="Register" ),
    path("Login", views.Login, name="Login"),
    path("Logout", views.Logout, name="Logout"),
    path("Importation", views.Importation, name="Importation"),
    path('Graph', views.Graph, name="Graph"),
]