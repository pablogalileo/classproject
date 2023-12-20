import requests
import json
import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats
from statsmodels.graphics.mosaicplot import mosaic
from scipy.stats import chi2_contingency


def main():
    st.title("Proyecto Final - Product Development")
    st.write("Pablo Méndez, Eddy Acuña")

    tab1, tab2, tab3 = st.tabs(["Random Forest", "Desition Tree", "Extra Tree Desition"])

    with tab1:
        st.header("Model: Random Forest")
        st.image("https://static.thenounproject.com/png/961660-200.png", width=200)
        rf_age = st.number_input("Age")
        rf_gender = st.text_input("Gender")
        rf_tenure = st.number_input("Tenure")
        rf_usage_frequency = st.number_input("Usage Frequency")
        rf_support_calls = st.number_input("Support Calls")
        rf_payment_delay = st.number_input("Payment Delay")
        rf_subscription_type = st.text_input("Subscription Type")
        rf_contract_length = st.text_input("Contract Length")
        rf_total_spend = st.number_input("Total Spend")
        rf_last_interaction = st.number_input("Last Interaction")
        entry={
            "Age": rf_age,
            "Gender": rf_gender,
            "Tenure": rf_tenure,
            "Usage Frequency": rf_usage_frequency,
            "Support Calls": rf_support_calls,
            "Payment Delay": rf_payment_delay,
            "Subscription Type": rf_subscription_type,
            "Contract Length": rf_contract_length,
            "Total Spend": rf_total_spend,
            "Last Interaction": rf_last_interaction
        }
        if st.button("Enviar datos al modelo RF", type="primary"):
            headers = {'Content-Type': 'application/json'}
            response = requests.post("http://127.0.0.1:5000/predict_rf/", json=entry, headers=headers)
            #st.write(response)
            data_table1 = pd.json_normalize(response.json())
            st.write(data_table1)

    with tab2:
        st.header("Model2: Desition Tree")
        st.image("https://cdn-icons-png.flaticon.com/512/5139/5139787.png", width=200)
        dt_age = st.number_input("dt Age")
        dt_gender = st.text_input("dt Gender")
        dt_tenure = st.number_input("dt Tenure")
        dt_usage_frequency = st.number_input("dt Usage Frequency")
        dt_support_calls = st.number_input("dt Support Calls")
        dt_payment_delay = st.number_input("dt Payment Delay")
        dt_subscription_type = st.text_input("dt Subscription Type")
        dt_contract_length = st.text_input("dt Contract Length")
        dt_total_spend = st.number_input("dt Total Spend")
        dt_last_interaction = st.number_input("dt Last Interaction")

        entry={
            "Age": dt_age,
            "Gender": dt_gender,
            "Tenure": dt_tenure,
            "Usage Frequency": dt_usage_frequency,
            "Support Calls": dt_support_calls,
            "Payment Delay": dt_payment_delay,
            "Subscription Type": dt_subscription_type,
            "Contract Length": dt_contract_length,
            "Total Spend": dt_total_spend,
            "Last Interaction": dt_last_interaction
        }
        if st.button("Enviar datos al modelo DT", type="primary"):
            headers = {'Content-Type': 'application/json'}
            response = requests.post("http://127.0.0.1:5000/predict_rf/", json=entry, headers=headers)
            #st.write(response)
            data_table1 = pd.json_normalize(response.json())
            st.write(data_table1)

    with tab3:
        st.header("Mode3: Extra Tree Desition")
        st.image("https://cdn-icons-png.flaticon.com/512/1809/1809086.png", width=200)
        et_age = st.number_input("et Age")
        et_gender = st.text_input("et Gender")
        et_tenure = st.number_input("et Tenure")
        et_usage_frequency = st.number_input("et Usage Frequency")
        et_support_calls = st.number_input("et Support Calls")
        et_payment_delay = st.number_input("et Payment Delay")
        et_subscription_type = st.text_input("et Subscription Type")
        et_contract_length = st.text_input("et Contract Length")
        et_total_spend = st.number_input("et Total Spend")
        et_last_interaction = st.number_input("et Last Interaction")


        entry={
            "Age": et_age,
            "Gender": et_gender,
            "Tenure": et_tenure,
            "Usage Frequency": et_usage_frequency,
            "Support Calls": et_support_calls,
            "Payment Delay": et_payment_delay,
            "Subscription Type": et_subscription_type,
            "Contract Length": et_contract_length,
            "Total Spend": et_total_spend,
            "Last Interaction": et_last_interaction
        }
        if st.button("Enviar datos al modelo ET", type="primary"):
            headers = {'Content-Type': 'application/json'}
            response = requests.post("http://127.0.0.1:5000/predict_rf/", json=entry, headers=headers)
            #st.write(response)
            data_table1 = pd.json_normalize(response.json())
            st.write(data_table1)

if(__name__=='__main__'):
    main()