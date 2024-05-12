import streamlit as st
from streamlit_option_menu import option_menu
import pymysql
import sqlalchemy
from sqlalchemy import create_engine
import plotly.express as pe
import pandas as pd
import mysql.connector
import os
import json
import requests


#sql connection:
mydb = mysql.connector.connect(host="localhost",user="root",password="",database="phonepe")
#print(mydb)
mycursor = mydb.cursor(buffered=True)

#function calling for list of state:
def state_list():
    mycursor.execute("SELECT DISTINCT States FROM phonepe.aggregated_trans ORDER BY States ASC;")
    s = mycursor.fetchall()
    original_state = [i[0] for i in s]
    return original_state

#Function call for data exploration:
def tran_a(year, quarter):
    # Connect to MySQL database and execute the SQL query
    query = '''SELECT States, SUM(Transaction_amount) AS Transaction_amount FROM phonepe.aggregated_trans WHERE Years =%s and Quater = %s  GROUP BY States'''
    mycursor.execute(query,(year, quarter))
    q1 = mycursor.fetchall()
    S1 = pd.DataFrame(q1,columns=("States","Transaction_Amount"))

    return S1

#function calling for choropleth map
def create_choropleth_a(trans1):
    fig = pe.choropleth(trans1,
                        geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                        featureidkey='properties.ST_NM',
                        locations="States",
                        color='Transaction_Amount',
                        color_continuous_scale="earth",
                        title="TRANSACTION AMOUNT",
                        height=600)
    fig.update_geos(fitbounds='locations', visible=False)
    return fig
    
    
def tran_c(year,quarter):
    query ='''SELECT States, SUM(Transaction_count) AS Transaction_Count FROM phonepe.aggregated_user 
                        WHERE Years = %s and Quater = %s GROUP BY States;'''
    mycursor.execute(query,(year, quarter))
    q2=mycursor.fetchall()
    S2=pd.DataFrame(q2,columns=("States","Transaction_Count"))

    return S2


def create_choropleth_c(trans2):
    fig1 = pe.choropleth(trans2, geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson", 
                        featureidkey='properties.ST_NM', 
                        locations='States', 
                        color='Transaction_Count', 
                        color_continuous_scale="earth", 
                        title="TRANSACTION COUNT", 
                        height=600)
    fig1.update_geos(fitbounds='locations', visible=False)

    return fig1

#function calling for data visualization:
def top_tran_a(year):
        mycursor.execute('''SELECT States, SUM(Transaction_amount) AS Transaction_Amount FROM phonepe.top_trans
                WHERE Years = %s GROUP BY States ORDER BY Transaction_amount DESC LIMIT 10''', (year,))
        
        q3=mycursor.fetchall()
        S3=pd.DataFrame(q3,columns=("States","Transaction_Amount"))

        return S3

def trans_type():
        mycursor.execute('''SELECT Transaction_type, SUM(Transaction_count) AS Total_Transaction_Count, SUM(Transaction_amount) AS Total_Transaction_Amount 
                                FROM phonepe.aggregated_trans GROUP BY Transaction_type''')
        q4=mycursor.fetchall()
        S4=pd.DataFrame(q4,columns=("Transaction_type","Total_Transaction_Count","Total_Transaction_Amount"))

        return S4

def tot_count():
        mycursor.execute('''SELECT States,Pincodes, SUM(Transaction_count) AS Total_Count FROM phonepe.top_trans 
                            WHERE Years = 2023 GROUP BY States ORDER BY Transaction_count DESC LIMIT 20''')
        q5=mycursor.fetchall()
        S5=pd.DataFrame(q5,columns=("States","Pincodes","Total_Count"))

        return S5

def state_am():
    mycursor.execute('''SELECT States, Years, Transaction_amount As Transaction_Amount  FROM phonepe.aggregated_trans 
                        WHERE Years BETWEEN 2018 AND 2023 GROUP BY States''')
    q6=mycursor.fetchall()
    S6=pd.DataFrame(q6,columns=("States","Years","Transaction_Amount"))

    return S6

def avg_reg(state):
    mycursor.execute('''SELECT Districts, AVG(RegisteredUser) AS Registered_User FROM phonepe.map_user WHERE States = %s
                        GROUP BY Districts ORDER BY RegisteredUser''', (state,))
    q7=mycursor.fetchall()
    S7=pd.DataFrame(q7,columns=("Districts","Registered_User"))
    return S7


def brand_per(year,quarter):
    query= '''SELECT States, SUM(Percentage) AS Percentage, Brands FROM phonepe.aggregated_user 
                        WHERE Years = %s AND Quater = %s GROUP BY States, Brands'''
    mycursor.execute(query,(year, quarter))
    q8=mycursor.fetchall()
    S8=pd.DataFrame(q8,columns=("States","Percentage","Brands"))
    return S8

def state_dis(year):
    mycursor.execute('''SELECT States, Transaction_type, SUM(Transaction_count) AS Transaction_Count FROM phonepe.aggregated_trans WHERE Years = %s Group by States,Transaction_type''', (year,))
    q9=mycursor.fetchall()
    S9=pd.DataFrame(q9,columns=("States","Transaction_type","Transaction_Count"))
    return S9

def tot_app(year, quarter):
    query = '''SELECT States, SUM(AppOpens) AS Total_AppOpens FROM phonepe.map_user WHERE Years = %s and Quater = %s GROUP BY States'''
    mycursor.execute(query,(year, quarter))
    q10 = mycursor.fetchall()
    S10 = pd.DataFrame(q10,columns=("States","Total_AppOpens"))
    return S10

def state_ac(state):
    mycursor.execute( '''SELECT States, Years, SUM(Transaction_amount) AS Transaction_Amount, SUM(Transaction_count) AS Transaction_Count FROM phonepe.aggregated_trans WHERE States = %s GROUP BY Years''', (state,))
    q11=mycursor.fetchall()
    S11=pd.DataFrame(q11,columns=("States","Years","Transaction_Amount","Transaction_Count"))
    return S11


#streamlit part:
with st.sidebar:

    select = option_menu("Main Menu",["HOME", "ABOUT","DATA EXPLORATION", "DATA VISUALIZATION"], 
    icons =["house","exclamation-circle","bar-chart-line","graph-up-arrow"],
    menu_icon= "menu-button-wide",
    default_index=0,
    styles={"nav-link": {"font-size": "20px", "text-align": "left", "margin": "-2px", "--hover-color": "#6F36AD"},
                        "nav-link-selected": {"background-color": "#6F36AD"}})


# MENU 1 - HOME
if select == "HOME":
    #st.set_page_config(layout= "wide")
    st.title(":violet[Phonepe Pulse Data Visualization and Exploration]")
    st.markdown('**(Note)**:-This data between **2018** to **2023** in **INDIA**')
    st.markdown("# :purple[Data Visualization and Exploration]")
    st.markdown("## :black[A User-Friendly Tool Using Streamlit and Plotly]")
    st.markdown("### :red[Domain :] Fintech")
    st.markdown("### :red[Technologies used :] Github Cloning, Python, Pandas, MySql, mysql-connector-python, Streamlit, and Plotly.")

#MENU -2 "ABOUT"
elif select == "ABOUT":
    st.title(":red[About Phonepe:]")
    st.write(" ")
    st.write(" ")
    st.image('phonepe.png')
    st.write(" ")
    st.write(" ")
    st.markdown("#### India with 89.5 million digital transactions in the year 2022 has topped the list of five countries in digital payments, according to data from MyGovIndia.")
    st.markdown("#### India is number one in digital payments. India is one of the countries where mobile data is the cheapest.")
    st.markdown("#### PhonePe is an Indian digital payments and financial services company headquartered in Bengaluru, Karnataka, India. ")
    



# MENU 3 - DATA EXPLORATION:
elif select == "DATA EXPLORATION":

    tab1, tab2 = st.tabs(["Transaction","User"])

    with tab1:

        st.title(":blue[Choropleth Map of Transaction Amount by State]")


        years = [2018, 2019, 2020, 2021, 2022, 2023]
        quarter = [1, 2, 3, 4]
    
        year_selected = st.selectbox(":green[Select the year:]", years, index = None, key="t1sb1")
        quarter_selected = st.selectbox(":green[Select the quarter:]", quarter, index = None, key="t1sb2")

        if year_selected != None and quarter_selected != None:
            trans1= tran_a(year_selected, quarter_selected)
            fig1 = create_choropleth_a(trans1)
            st.plotly_chart(fig1,use_container_width=True)
            st.dataframe(trans1)

    with tab2:

        st.title(":blue[Choropleth Map of Total Transaction User Count by State]")
        
        years = [2018, 2019, 2020, 2021, 2022, 2023]
        quarter = [1, 2, 3, 4]
    
        year_selected = st.selectbox(":green[Select the year:]", years, index = None, key="t2sb1")
        quarter_selected = st.selectbox(":green[Select the quarter:]", quarter, index = None, key="t2sb2")

        if year_selected != None and quarter_selected != None:
            trans2= tran_c(year_selected, quarter_selected)
            fig1= create_choropleth_c(trans2)
            st.plotly_chart(fig1,use_container_width=True)
            st.dataframe(trans2)
        

    
# MENU 4 - DATA VISUALIZATION:
elif select == "DATA VISUALIZATION":
    st.title(':green[Insights]')
    Query = ['Select your Question',
    '1.What are the Top 10 Transaction Amounts by State?', 
    '2.What are the Transaction Types of Analysis?', 
    '3.What are the top 20 States with Highest Total count in 2023?',
    '4.what are the State wise transaction amount between 2018 and 2023?',
    '5.What are the State wise transaction count distribution?',
    '6.What is the average of Registered user?',
    '7.What are the brand wise user percentage?',
    '8.what are the total Appopens by statewise?',
    '9.What are the statewise transaction amount and transaction count in line plotly chart?']


    Selected_Query = st.selectbox(' ',options = Query)
    if Selected_Query =='1.What are the Top 10 Transaction Amounts by State?':

        year = st.selectbox("Select the year:", [2018, 2019, 2020, 2021, 2022, 2023],index = None)

        if year != None:
            top_a = top_tran_a(year)

            fig_amt = pe.bar(top_a, x="States", y="Transaction_Amount", title=f'Top 10 Transaction Amounts by State for {year}',color_discrete_sequence=pe.colors.sequential.Aggrnyl_r, height= 700,width= 650)

            st.plotly_chart(fig_amt)

        
    if Selected_Query =='2.What are the Transaction Types of Analysis?':
        tr_ty=trans_type()

        #Pie chart showing total transaction count by type
        fig_type = pe.pie(tr_ty, values='Total_Transaction_Count', names='Transaction_type', color_discrete_sequence= pe.colors.sequential.RdBu, title='Total Transaction Count by Type',hole= 0.5)
        st.plotly_chart(fig_type)

        #Pie chart showing total transaction amount by type
        fig_type1 = pe.pie(tr_ty, values='Total_Transaction_Amount', names='Transaction_type', color_discrete_sequence= pe.colors.sequential.RdBu, title='Total Transaction Amount by Type',hole= 0.5)
        st.plotly_chart(fig_type1)


    if Selected_Query =='3.What are the top 20 States with Highest Total count in 2023?':
        tot_count_s = tot_count()

        fig_amt1 = pe.bar(tot_count_s, x="States", y="Total_Count", title=f'Highest Total count 2023', color_discrete_sequence=pe.colors.sequential.Blackbody, height= 700,width= 650)
        st.plotly_chart(fig_amt1)

        st.dataframe(tot_count_s)
        
    if Selected_Query =='4.what are the State wise transaction amount in Billions for year 2018?':

        state_am_s = state_am()

        fig_amt2 = pe.bar(state_am_s, x="States", y="Transaction_Amount", title=f'State Wise Transaction Amount',color_discrete_sequence=pe.colors.sequential.haline, height= 700,width= 650)

        st.plotly_chart(fig_amt2)


    if Selected_Query =='5.What are the State wise transaction count distribution?':

        year = st.selectbox("Select the year:", [2018, 2019, 2020, 2021, 2022, 2023],index = None)

        if year != None:
            dis_st = state_dis(year)

            fig_amt5 = pe.bar(dis_st, x="States", y="Transaction_Count", title=f'State wise transaction count',color_discrete_sequence=pe.colors.sequential.RdBu, height= 700,width= 650)
            st.plotly_chart(fig_amt5)

            st.dataframe(dis_st)


    if Selected_Query =='6.What is the average of Registered user?':

        states = state_list()
        
        state = st.selectbox("Select the State:", states, index = None)

        if state != None:
            avg_re_us = avg_reg(state)

            fig_amt4 = pe.bar(avg_re_us, x="Districts", y="Registered_User", title=f'AVERAGE REGISTERED USER',color_discrete_sequence=pe.colors.sequential.Burg, height= 700,width= 650)
            st.plotly_chart(fig_amt4)


    if Selected_Query =='7.What are the brand wise user percentage?':
        
        years = [2018, 2019, 2020, 2021, 2022, 2023]
        quarter = [1, 2, 3, 4]
    
        year_selected = st.selectbox(":green[Select the year:]", years, index = None, key="t3sb3")
        quarter_selected = st.selectbox(":green[Select the quarter:]", quarter, index = None, key="t3sb4")

        if year_selected != None and quarter_selected != None:
            br_per=brand_per(year_selected, quarter_selected)

            fig_type4 = pe.pie(br_per, values='Percentage', names='Brands', color_discrete_sequence= pe.colors.sequential.tempo, title='Brand wise of User Percentage',hole= 0.5)
            st.plotly_chart(fig_type4)
        
    if Selected_Query =='8.what are the total Appopens by statewise?':

        years = [2018, 2019, 2020, 2021, 2022, 2023]
        quarter = [1, 2, 3, 4]
    
        year_selected = st.selectbox(":green[Select the year:]", years, index = None, key="t4sb4")
        quarter_selected = st.selectbox(":green[Select the quarter:]", quarter, index = None, key="t4sb5")

        if year_selected != None and quarter_selected != None:
            appop_user = tot_app(year_selected, quarter_selected)

            fig_amt6 = pe.bar(appop_user, x="States", y="Total_AppOpens", title=f'Statewise Total AppOpens',color_discrete_sequence=pe.colors.sequential.turbid, height= 700,width= 650)
            st.plotly_chart(fig_amt6)
            
    if Selected_Query =='9.What are the statewise transaction amount and transaction count in line plotly chart?':

        states = state_list() 

        
        state = st.selectbox("Select the State:", states, index = None)

        if state != None:
            st_amct = state_ac(state)

            fig_line = pe.line(st_amct, x='Years', y='Transaction_Amount', color='States', title='Transaction Amount By State Across Years',
                labels={'Years': 'Year', 'Transaction_Amount': 'Transaction Amount'})

            st.plotly_chart(fig_line)

            fig_line1 = pe.line(st_amct, x='Years', y='Transaction_Count', color='States', title='Transaction Count By State Across Years',
                labels={'Years': 'Year', 'Transaction_Count': 'Transaction Count'})

            st.plotly_chart(fig_line1)
            st.dataframe(st_amct)

