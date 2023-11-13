import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats
from summarytools import dfSummary

def tab_one():
    
    st.title("Descripción")
    st.write("Esta es una aplicación que permite cargar y explorar un dataset completo como preparación para un mecanismo de entrenamiento automático.")

    dataset = st.file_uploader("Upload a file", type=["csv", "xlsx"])

    if dataset is not None:
        ss = st.session_state
        st.session_state.dataset_name = dataset.name
        st.session_state.dataset = None

        if dataset.type == "application/vnd.ms-excel":  # XLSX file
            st.session_state.dataset = pd.read_excel(dataset, engine="openpyxl")
        elif dataset.type == "text/csv":  # CSV file
            st.session_state.dataset = pd.read_csv(dataset)
        else:
            st.warning("Unsupported file format. Please upload a CSV or XLSX file.")
            return

        st.success(f"El archivo '{dataset.name}' fue cargado correctamente")

        st.dataframe(st.session_state.dataset)


def tab_two():

    st.title("Análisis exploratorio de los datos")
    ss = st.session_state

    if "dataset_name" in st.session_state:
        st.write(f"Dataset cargado: {ss.dataset_name}")
        st.dataframe(ss.dataset.head())

    else:
        st.warning("Por favor cargue un archivo de datos en la pestaña correspondiente")


    st.markdown("### Variables")
        
    #Encontramos cada tipo de variable

    categoricas = []
    for col in ss.dataset.columns:
        if((ss.dataset[col].dtype=='object')or(ss.dataset[col].dtype=='string')):
            categoricas.append(col)

    continuas = []
    discretas = []
    numericas = []
    for col in ss.dataset.columns:
        if((ss.dataset[col].dtype=='float64')or(ss.dataset[col].dtype=='int64')):
            numericas.append(col)
            if(len(ss.dataset[col].unique()) <= 30):
                continuas.append(col)
            else:
                discretas.append(col)

    fechas = []
    for col in ss.dataset.columns:
        if((ss.dataset[col].dtype=='datetime')or(ss.dataset[col].dtype=='timedelta')):
            fechas.append(col)

    # Creamos un arreglo para colocar las 4 tablas juntas
    col1, col2, col3, col4 = st.columns(4)

    col1.dataframe(pd.DataFrame({"Continuas": continuas}))
    col2.dataframe(pd.DataFrame({"Discretas": discretas}))
    col3.dataframe(pd.DataFrame({"Categoricas": categoricas}))
    col4.dataframe(pd.DataFrame({"Fechas": fechas}))


    columns = ss.dataset.columns.tolist()

    st.markdown("#### Seleccione una variable para ver la descripción estadística")

    variable = st.selectbox("Variable",columns)

    if variable in categoricas:
        col1, col2 = st.columns(2)
        col1.dataframe(ss.dataset[variable].value_counts())
        col2.dataframe(ss.dataset[variable].describe())
        # Count the occurrences of each category
        category_counts = ss.dataset[variable].value_counts()
        fig, ax = plt.subplots()
        sns.barplot(x=category_counts.index, y=category_counts.values, ax=ax)
        plt.xlabel(variable)
        plt.ylabel("Count")
        plt.title(f"Bar Chart of {variable}")
        st.pyplot(fig)
    else: 
        st.dataframe(ss.dataset[variable].describe())
        mean_val = ss.dataset[variable].mean()
        median_val = ss.dataset[variable].median()
        std_dev_val = ss.dataset[variable].std()
        variance_val = ss.dataset[variable].var()
        mode_val = ss.dataset[variable].mode().iloc[0]

        st.write(f"Mediana: {median_val}")
        st.write(f"Varianza: {variance_val}")
        st.write(f"Moda: {mode_val}")
       
        # Histograma
        fig, ax = plt.subplots()
        sns.histplot(ss.dataset[variable], kde=True, bins='auto', color='blue', ax=ax)
        ax.axvline(mean_val, color='red', linestyle='dashed', linewidth=2, label='Mean')
        ax.axvline(median_val, color='green', linestyle='dashed', linewidth=2, label='Median')
        ax.axvline(std_dev_val, color='orange', linestyle='dashed', linewidth=2, label='Std Dev')
        ax.axvline(variance_val, color='purple', linestyle='dashed', linewidth=2, label='Variance')
        ax.axvline(mode_val, color='brown', linestyle='dashed', linewidth=2, label='Mode')

        ax.set_xlabel(variable)
        ax.set_ylabel("Frecuencia")
        ax.legend()
        ax.set_title(f"Histograma de {variable}")

        st.pyplot(fig)

        #Minimo y maximo de la variable
        min = ss.dataset[variable].min()
        max = ss.dataset[variable].max()
        kde = stats.gaussian_kde(ss.dataset[variable])

        rangos = st.slider(f"Valor de {variable}",min,max,(min,max))
        r_min = rangos[0]
        r_max = rangos[1]

        fig,ax = plt.subplots(figsize=(10,4))
        sns.kdeplot(ss.dataset[variable], color="green",shade=True)
        #dataset[variable].plot.density(color="blue")

        kde_value_min = kde.evaluate(r_min)
        ax.vlines(x=r_min,ymin=0,ymax=kde_value_min)

        kde_value_max = kde.evaluate(r_max)
        ax.vlines(x=r_max,ymin=0,ymax=kde_value_max) 

        x = np.linspace(r_min,r_max,1000) #1000 lineas entre min y max
        y = kde.evaluate(x)
        ax.fill_between(x,y,color="orange",alpha=0.5)


        plt.title(f'Densidad de {variable}')
        st.pyplot(fig)

        st.text(f'Probabilidad: {np.round(np.sum(y),4)/100}')


    
    fig2 = plt.figure(figsize=(10,4))
    variableA = st.selectbox("Variable Numerica", numericas)
    variableB = st.selectbox("Variable Categorica", categoricas)

    sns.boxplot(data=ss.dataset, x=variableB, y=variableA)
    st.pyplot(fig2)


def main():
    st.title("Proyecto 1 - Product Development")
    st.write("Pablo Méndez, Eddy Acuña")

    selected_tab = st.selectbox("Seleccione una pestaña", ["Carga archivo de datos", "Análisis exploratorio de los datos"])

    if selected_tab == "Carga archivo de datos":
        tab_one()
    elif selected_tab == "Análisis exploratorio de los datos":
        tab_two()



if(__name__=='__main__'):
    main()