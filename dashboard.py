from datetime import datetime
from tempfile import template
import mysql.connector
import streamlit as st
import pandas as pd
import plotly.express as px
import warnings
import json
import plotly.graph_objects as go
import pydeck as pdk
from sqlalchemy import create_engine, distinct
import diskcache as dc

warnings.filterwarnings("ignore")

st.set_page_config(page_title="NMCZ Dashboard", page_icon=":bar_chart:", layout="wide")

st.markdown("<br><br>", unsafe_allow_html=True)
st.title(" :bar_chart: Nursing and Midwife Council of Zambia Dashboard")

st.markdown('<style>div.block-container{padding-top:1rem;padding-bottom:1rem;}</style>', unsafe_allow_html=True)

# # Initialize connection.
# conn1 = st.connection("gncz_dbms", type="sql", connect_args={
#    "ssl": {
#        "ca": st.secrets["connections"]["gncz_dbms"]["ssl_ca"]
#    }
#})

cache = dc.Cache("cache")

conn1 = mysql.connector.connect(
    host=st.secrets["connections"]["gncz_dbms"]["host"],
    user=st.secrets["connections"]["gncz_dbms"]["username"],
    password=st.secrets["connections"]["gncz_dbms"]["password"],
    database=st.secrets["connections"]["gncz_dbms"]["database"],
    port=st.secrets["connections"]["gncz_dbms"]["port"],
    ssl_ca=st.secrets["connections"]["gncz_dbms"]["ssl_ca"]
)

conn2 = mysql.connector.connect(
    host=st.secrets["connections"]["zambia_osp"]["host"],
    user=st.secrets["connections"]["zambia_osp"]["username"],
    password=st.secrets["connections"]["zambia_osp"]["password"],
    database=st.secrets["connections"]["zambia_osp"]["database"],
    port=st.secrets["connections"]["zambia_osp"]["port"],
    ssl_ca=st.secrets["connections"]["gncz_dbms"]["ssl_ca"]
)
# conn2 = st.connection("zambia_osp", type="sql", connect_args={
#     "ssl": {
#         "ca": st.secrets["connections"]["gncz_dbms"]["ssl_ca"],
#     }
# })

# df = conn1.query("SELECT * FROM gncz_dbms.indextbl AS a INNER JOIN gncz_dbms.assignprogramtbl AS b ON a.IndexID=b.IndexID INNER JOIN gncz_dbms.coursetbl AS c ON b.CourseID = c.CourseID INNER JOIN gncz_dbms.traininginstitutiontbl AS d ON b.InstitutionID = d.InstitutionID")
# df = conn1.query("SELECT * FROM gncz_dbms.indextbl AS a INNER JOIN gncz_dbms.assignprogramtbl AS b ON a.IndexID=b.IndexID INNER JOIN gncz_dbms.coursetbl AS c ON b.CourseID = c.CourseID INNER JOIN gncz_dbms.traininginstitutiontbl AS d ON b.InstitutionID = d.InstitutionID inner join gncz_dbms.districttbl e on d.DistrictID=e.DistrictID inner join gncz_dbms.provincetbl as f on e.ProvinceID=f.ProvinceID", ttl=604800)
# df2 = conn1.query("select a.IndexID, a.DateOfBirth,  a.Gender, b.AssignProgramID, e.CourseDesc, c.RegistrationDate, d.WorkStationID, d.LicenseYear, d.FromDate, d.ToDate  from gncz_dbms.indextbl as a inner join gncz_dbms.assignprogramtbl as b  inner join gncz_dbms.registrationtbl c on b.AssignProgramID = c.AssignProgramID inner join gncz_dbms.retentiontbl as d on c.RegistrationID = d.RegistrationID inner join gncz_dbms.coursetbl as e on b.CourseID = e.CourseID", ttl=604800)
# df3 = conn1.query("select a.WorkStationName, a.WorkStationID, b.DistrictDesc, c.ProvinceDesc, d.FacilityAgent from gncz_dbms.workstationstbl as a inner join gncz_dbms.districttbl as b on a.DistrictID = b.DistrictID inner join gncz_dbms.provincetbl as c on b.ProvinceID = c.ProvinceID inner join gncz_dbms.facilityagenttbl as d on a.FacilityAgentID = d.FacilityAgentID", ttl=604800)

# df0 = conn1.query("select IndexID, DateOfBirth, Gender from gncz_dbms.indextbl", ttl=604800)
# df00 = conn1.query("select IndexID, AssignProgramID, InstitutionID, CourseID, CommenceDate, CompletionDate from gncz_dbms.assignprogramtbl", ttl=604800)
# df01 = conn1.query("select ProvinceID, ProvinceDesc from gncz_dbms.provincetbl", ttl=604800)
# df02 = conn1.query("select DistrictID, DistrictDesc, ProvinceID from gncz_dbms.districttbl;", ttl=604800)
# df03 = conn1.query("select WorkStationID, WorkStationName, DistrictID from gncz_dbms.workstationstbl", ttl=604800)
# df04= conn1.query("select InstitutionID, InstitutionName, DistrictID from gncz_dbms.traininginstitutiontbl", ttl=604800)
# df05 = conn1.query("select CourseID, CourseDesc, CourseDuration from gncz_dbms.coursetbl",  ttl=604800)
# df06 = conn1.query("select RegistrationID, WorkStationID, LicenseYear, FromDate, ToDate from gncz_dbms.retentiontbl where VerificationStatus='A'", ttl=604800)
# df07 = conn1.query("select RegistrationID, AssignProgramID, RegistrationDate from gncz_dbms.registrationtbl", ttl= 604800)
df08 = pd.read_csv('mfl.csv',  delimiter=';')


def fetch_from_cache_or_db(query, cache_key, ttl=86400):
    # Try to get the data from cache
    cached_data = cache.get(cache_key)
    if cached_data is not None:
        return pd.DataFrame(cached_data)
    
    cursor = conn1.cursor(dictionary=True)
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    
    cache.set(cache_key, result, expire=ttl)
    return pd.DataFrame(result)

df0 = fetch_from_cache_or_db("SELECT IndexID, DateOfBirth, Gender FROM gncz_dbms.indextbl", "df0")
df00 = fetch_from_cache_or_db("SELECT IndexID, AssignProgramID, InstitutionID, CourseID, CommenceDate, CompletionDate FROM gncz_dbms.assignprogramtbl", "df00")
df01 = fetch_from_cache_or_db("SELECT ProvinceID, ProvinceDesc FROM gncz_dbms.provincetbl", "df01")
df02 = fetch_from_cache_or_db("SELECT DistrictID, DistrictDesc, ProvinceID FROM gncz_dbms.districttbl;", "df02")
df03 = fetch_from_cache_or_db("SELECT WorkStationID, WorkStationName, DistrictID FROM gncz_dbms.workstationstbl", "df03")
df04 = fetch_from_cache_or_db("SELECT InstitutionID, InstitutionName, DistrictID FROM gncz_dbms.traininginstitutiontbl", "df04")
df05 = fetch_from_cache_or_db("SELECT CourseID, CourseDesc, CourseDuration FROM gncz_dbms.coursetbl", "df05")
df06 = fetch_from_cache_or_db("SELECT RegistrationID, WorkStationID, LicenseYear, FromDate, ToDate FROM gncz_dbms.retentiontbl WHERE VerificationStatus='A'", "df06")
df07 = fetch_from_cache_or_db("SELECT RegistrationID, AssignProgramID, RegistrationDate FROM gncz_dbms.registrationtbl", "df07")

conn1.close()

df06['ToDate'] = pd.to_datetime(df06['ToDate'])
df07['RegistrationDate'] = pd.to_datetime(df07['RegistrationDate'])


# df07['RegistrationYear'] = df07['RegistrationDate'].dt.year.fillna(0).astype(int)



df = pd.merge(df0, df00, on='IndexID')

df = pd.merge(df, df04, on='InstitutionID')

# Merge the result with df02 on DistrictID
df = pd.merge(df, df02, on='DistrictID')

# Merge the result with df01 on ProvinceID
df = pd.merge(df, df01, on='ProvinceID')

# Merge the result with df05 on CourseID
df = pd.merge(df, df05, on='CourseID')

df2 = pd.merge(df0, df00, on='IndexID')
df2 = pd.merge(df2, df07, on='AssignProgramID')
df2 = pd.merge(df2, df05, on='CourseID')
df2= pd.merge(df2, df06, on='RegistrationID')
df2 = pd.merge(df2, df03, on='WorkStationID')
df2 = pd.merge(df2, df02, on='DistrictID')
df2 = pd.merge(df2, df01, on='ProvinceID')
df2 = df2.drop_duplicates(subset=['IndexID'])

df3 = pd.merge(df07, df00, on='AssignProgramID')
df3 = pd.merge(df3, df05, on='CourseID')
df3 = df3.dropna(subset=['RegistrationDate'])
df3['Year'] = df3['RegistrationDate'].dt.year.astype(int)
registrations_per_year = df3.groupby('Year').size().reset_index(name='Registrations')


df4 = pd.merge(df06, df07, on='RegistrationID')
df4 = pd.merge(df4, df00, on='AssignProgramID')
df4 = pd.merge(df4, df05, on='CourseID')
df4['Year'] = df06['ToDate'].dt.year.astype(int)
renewals_per_year = df4.groupby('Year').size().reset_index(name='Renewals')

df5 = df3.drop_duplicates(subset=['IndexID'])
total_registered_per_year = df5.groupby('Year').size().reset_index(name='Total Registered')
total_registered_per_year['Registered'] = total_registered_per_year['Total Registered'].cumsum()
total_registered_per_year = total_registered_per_year[total_registered_per_year['Year'] >= 2015]
# print(total_registered_per_year)

#print(registrations_per_year)
# print(renewals_per_year)

time_df = pd.merge(renewals_per_year, registrations_per_year, on='Year', how='outer').fillna(0)
time_df = time_df[time_df['Year'] >= 2015]

registered_renewal_df = pd.merge(renewals_per_year, total_registered_per_year, on='Year', how='outer').fillna(0)
registered_renewal_df = registered_renewal_df[registered_renewal_df['Year'] >= 2015]


# Example metrics
df07 = df07.dropna(subset=['RegistrationDate'])
total_nurses_merged = pd.merge(df00, df07, on='AssignProgramID')
total_nurses_filtered = total_nurses_merged.drop_duplicates(subset='IndexID')
total_nurses = total_nurses_filtered['IndexID'].nunique()
cleaned_df= pd.merge(df0, total_nurses_filtered, on='IndexID')
cleaned_df['Age'] = datetime.now().year - pd.to_datetime(cleaned_df['DateOfBirth'], errors='coerce').dt.year
average_age = cleaned_df['Age'].mean()
total_count = len(cleaned_df)
female_count = cleaned_df[cleaned_df['Gender'] == 'F'].shape[0]
female_percentage = (female_count / total_count) * 100

hnp_nurses = df3[df3['CourseID'] == 44]
hnp_nurses = (hnp_nurses['IndexID'].nunique()/total_count) * 100

bachelor_nurses = df3[df3['CourseDesc'].str.contains('BACHELOR', na=False)]
bachelor_nurses= bachelor_nurses.drop_duplicates(subset='IndexID')
bachelor_nurses = (bachelor_nurses['IndexID'].nunique()/total_count) * 100

df_retentions = df06.copy()
df_retentions = df_retentions[df_retentions['ToDate'].dt.year == 2024]
df_facilities = pd.merge(df_retentions, df08, on='WorkStationID')
df_facilities = df_facilities.dropna(subset=['Latitude', 'Longitude'])
df_facilities = df_facilities.groupby(['WorkStationID', 'WorkStationName', 'Latitude', 'Longitude']).size().reset_index(name='Count')
# df_facilities['Latitude'] = df_facilities['Latitude'].str.replace(',', '.', regex=False)
# df_facilities['Latitude'] = pd.to_numeric(df_facilities['Latitude'], errors='coerce')

# df_facilities['Latitude'] = pd.to_numeric(df_facilities['Latitude'], errors='coerce')
# df_facilities['Longitude'] = pd.to_numeric(df_facilities['Longitude'], errors='coerce')

# Drop rows with NaN values in 'Latitude' or 'Longitude' columns
# df_facilities = df_facilities.dropna(subset=['Latitude', 'Longitude'])
# df_facilities = df_facilities.drop_duplicates(subset=['WorkStationID'])
# df_facilities.loc[df_facilities['WorkStationID'] == 34.0, 'Latitude'] = -13.628105

# df_facilities['Longitude'] = df_facilities['Longitude'].str.replace(',', '.', regex=False)
# df_facilities['Longitude'] = pd.to_numeric(df_facilities['Longitude'], errors='coerce')
# print(df_facilities)
# with open('custom.geo.json', encoding='utf-8') as f:
#     county_geojson = json.load(f)
#
# # Set the initial view
# view_state = pdk.ViewState(
#     latitude=df_facilities['Latitude'].mean(),
#     longitude=df_facilities['Longitude'].mean(),
#     zoom=5,
#     pitch=0
# )
#
# facility_layer = pdk.Layer(
#     "ScatterplotLayer",
#     data=df_facilities,
#     get_position='[longitude, latitude]',
#     get_fill_color='[0, 0, 255, 160]',  # Blue color with transparency
#     get_radius=5000,
#     pickable=True
# )
#
# # Create a layer for county boundaries
# county_layer = pdk.Layer(
#     "GeoJsonLayer",
#     data=county_geojson,
#     stroked=True,
#     filled=False,
#     get_line_color=[255, 0, 0],
#     line_width_min_pixels=2
# )


# non_numeric_latitude = df_facilities[~df_facilities['Latitude'].apply(lambda x: str(x).replace('.', '', 1).isdigit())]
# non_numeric_longitude = df_facilities[~df_facilities['Longitude'].apply(lambda x: str(x).replace('.', '', 1).isdigit())]
#
# print("Non-numeric Latitude values:\n", non_numeric_latitude)
# print("Non-numeric Longitude values:\n", non_numeric_longitude)


fig = go.Figure()
cl1, cl2, cl3,cl4, cl5 = st.columns((5))


df["CommenceDate"] = pd.to_datetime(df["CommenceDate"], errors='coerce')

startDate = pd.to_datetime(df["CommenceDate"]).min()
endDate = pd.to_datetime(df["CommenceDate"]).max()

df2['Age'] = datetime.now().year - pd.to_datetime(df2['DateOfBirth'], errors='coerce').dt.year

bins = [0, 20, 30, 40, 50, 60, 70, 80, 100]  # Adjust as needed
labels = ["0-20", "20-30", "30-40", "40-50", "50-60", "60-70", "70-80", "80+"]


df2['AgeGroup'] = pd.cut(df2['Age'], bins=bins, labels=labels, right=False)
df2['Gender'] = df2['Gender'].map({'M': 'Male', 'F': 'Female'})

# df4 = merged_df = pd.merge(df2, df3, on='WorkStationID')
col1, col2 = st.columns((2))
column1 = st.columns(1)[0]
with col1:
    date1 = pd.to_datetime(st.date_input("Start Date", startDate), errors='coerce')

with col2:
    date2 = pd.to_datetime(st.date_input("End Date", endDate), errors='coerce')

df = df[(df["CommenceDate"] >= date1) & (df["CommenceDate"] <= date2)].copy()

st.sidebar.header("Select your Filter: ")

province = st.sidebar.multiselect("Pick a Province", df["ProvinceDesc"].unique())

filtered_df = df.copy()
nurseWork_df = df2.copy()

age_distribution = nurseWork_df['AgeGroup'].value_counts().reset_index()
age_distribution.columns = ['AgeGroup', 'Count']



gender_distribution = nurseWork_df['Gender'].value_counts().reset_index()
gender_distribution.columns = ['Gender', 'Count']

region_distribution = nurseWork_df['ProvinceDesc'].value_counts().reset_index()
region_distribution.columns = ['ProvinceDesc', 'Count']


if province:
    filtered_df = filtered_df[filtered_df["ProvinceDesc"].isin(province)]
    nurseWork_df = nurseWork_df[nurseWork_df["ProvinceDesc"].isin(province)]
    age_distribution = nurseWork_df['AgeGroup'].value_counts().reset_index()
    age_distribution.columns = ['AgeGroup', 'Count']
    gender_distribution = nurseWork_df['Gender'].value_counts().reset_index()
    gender_distribution.columns = ['Gender', 'Count']
    region_distribution = nurseWork_df['ProvinceDesc'].value_counts().reset_index()
    region_distribution.columns = ['ProvinceDesc', 'Count']

district = st.sidebar.multiselect("Pick a District", filtered_df["DistrictDesc"].unique())
if district:
    filtered_df = filtered_df[filtered_df["DistrictDesc"].isin(district)]
    nurseWork_df = nurseWork_df[nurseWork_df["DistrictDesc"].isin(district)]
    age_distribution = nurseWork_df['AgeGroup'].value_counts().reset_index()
    age_distribution.columns = ['AgeGroup', 'Count']
    gender_distribution = nurseWork_df['Gender'].value_counts().reset_index()
    gender_distribution.columns = ['Gender', 'Count']
    region_distribution = nurseWork_df['ProvinceDesc'].value_counts().reset_index()
    region_distribution.columns = ['ProvinceDesc', 'Count']

# institution = st.sidebar.multiselect("Pick an Institution", filtered_df["InstitutionName"].unique())
# if institution:
#     filtered_df = filtered_df[filtered_df["InstitutionName"].isin(institution)]

program = st.sidebar.multiselect("Pick a Program", filtered_df["CourseDesc"].unique())
if program:
    filtered_df = filtered_df[filtered_df["CourseDesc"].isin(program)]
    nurseWork_df = nurseWork_df[nurseWork_df["CourseDesc"].isin(program)]
    age_distribution = nurseWork_df['AgeGroup'].value_counts().reset_index()
    age_distribution.columns = ['AgeGroup', 'Count']
    gender_distribution = nurseWork_df['Gender'].value_counts().reset_index()
    gender_distribution.columns = ['Gender', 'Count']
    region_distribution = nurseWork_df['ProvinceDesc'].value_counts().reset_index()
    region_distribution.columns = ['ProvinceDesc', 'Count']
    df3 = df3[df3["CourseDesc"].isin(program)]
    registrations_per_year = df3.groupby('Year').size().reset_index(name='Registrations')
    df4 = df4[df4["CourseDesc"].isin(program)]
    renewals_per_year = df4.groupby('Year').size().reset_index(name='Renewals')
    df5 = df5[df5["CourseDesc"].isin(program)]

    total_registered_per_year = df5.groupby('Year').size().reset_index(name='Total Registered')
    total_registered_per_year['Registered'] = total_registered_per_year['Total Registered'].cumsum()
    total_registered_per_year = total_registered_per_year[total_registered_per_year['Year'] >= 2015]

    registered_renewal_df = pd.merge(renewals_per_year, total_registered_per_year, on='Year', how='outer').fillna(0)
    registered_renewal_df = registered_renewal_df[registered_renewal_df['Year'] >= 2015]



coursecount_df = filtered_df.groupby("CourseDesc").size().reset_index(name='Count')

nursesperschool_df = filtered_df.groupby('InstitutionName').size().reset_index(name='count')

with cl1:
    fig_total_nurses = go.Figure(go.Indicator(
        mode="number",
        value=total_nurses,
        number={'valueformat': 'f'},
        title={"text": "Total Registered Nurses"}))
    st.plotly_chart(fig_total_nurses, use_container_width=True)

# Average Age
with cl2:
    fig_average_age = go.Figure(go.Indicator(
        mode="number",
        value=average_age,
        title={"text": "Average Age"}))
    st.plotly_chart(fig_average_age, use_container_width=True)

# Male Percentage
with cl3:
    fig_male_percentage = go.Figure(go.Indicator(
        mode="number",
        value=female_percentage,
        title={"text": "Female Percentage"},
        number={"suffix": "%"}))
    st.plotly_chart(fig_male_percentage, use_container_width=True)

with cl4:
    fig_male_percentage1 = go.Figure(go.Indicator(
        mode="number",
        value=hnp_nurses,
        title={"text": "HIV Nurse Practitioners"},
        number={"suffix": "%"}))
    st.plotly_chart(fig_male_percentage1, use_container_width=True)

with cl5:
    fig_male_percentage5 = go.Figure(go.Indicator(
        mode="number",
        value=bachelor_nurses,
        title={"text": "Percentage With Bachelors"},
        number={"suffix": "%"}))
    st.plotly_chart(fig_male_percentage5, use_container_width=True)


with col1:
    st.subheader("Number of Nurses Trained per Institution")
    # fig = px.bar(nursesperschool_df, x = "InstitutionName", y="CourseDesc", template="seaborn")
    # st.plotly_chart(fig, use_container_width=True, height = 200)
    fig = px.bar(nursesperschool_df, x='InstitutionName', y='count', labels={
        'InstitutionName': 'Institution Name',
        'count': 'Number of Nurses Trained'
    })
    st.plotly_chart(fig, use_container_width=True, height = 200)

    with st.expander("Facility ViewData"):
        st.write(nursesperschool_df.style.background_gradient(cmap="Blues"))
        csv = nursesperschool_df.to_csv(index = False).encode('utf-8')
        st.download_button("Download Data", data = csv, file_name = "schools.csv", mime = "text/csv",
                           help = "Click here to download the facility count data")

    st.subheader("Age Group Distribution")
    fig_age_pie = px.pie(age_distribution, values='Count', names='AgeGroup', hole=0.5)
    st.plotly_chart(fig_age_pie)

    st.subheader("Provincial Distribution")
    fig_region_bar = px.bar(region_distribution, x='ProvinceDesc', y='Count',
                            labels={'ProvinceDesc': 'Province', 'Count': 'Number of Nurses'})
    fig_region_bar.update_layout(barmode='group', yaxis=dict(tickformat=","))
    st.plotly_chart(fig_region_bar)

    st.subheader("License Renewals vs  Total Registered")
    fig = px.line(registered_renewal_df , x='Year', y=['Renewals', 'Registered'],
                  labels={'value': 'Number of Nurses', 'variable': 'Category'},
                  title='Annual Nurse License Renewals vs Registrations')
    fig.update_layout(barmode='group', yaxis=dict(tickformat=","))
    st.plotly_chart(fig, use_container_width=True, height=200)

    # st.subheader("License Renewals vs Registrations")
    # fig = px.line(time_df, x='Year', y=['Renewals', 'Registrations'],
    #               labels={'value': 'Count', 'variable': 'Category'},
    #               title='Annual Nurse License Renewals vs Registrations')
    # st.plotly_chart(fig,  use_container_width=True, height = 200)

    # mapped = pdk.Deck(
    #     layers=[county_layer, facility_layer],
    #     initial_view_state=view_state,
    #     tooltip={"text": "Facility: {facility_name}\nNurses: {Count}"}
    # )
    #
    # st.pydeck_chart(mapped)



with col2:
    st.subheader("Nurse Distributions Per Program")
    fig = px.pie(coursecount_df, values='Count', names='CourseDesc', hole=0.5)
    st.plotly_chart(fig,  use_container_width=True, height = 200)

    with st.expander("Programs View Data"):
        st.write(coursecount_df.style.background_gradient(cmap="Purples"))
        csv = coursecount_df.to_csv(index = False).encode('utf-8')
        st.download_button("Download Data", data = csv, file_name = "programs.csv", mime = "text/csv",
                           help = "Click here to download the program count data")

    st.subheader("Distribution of Nurses and Midwives by Gender")
    fig_gender_pie = px.pie(gender_distribution, values='Count', names='Gender',hole=0.5)
    st.plotly_chart(fig_gender_pie)

    # Gender Distribution - Bar Chart
    st.subheader("Distribution of Nurses and Midwives by Gender")
    fig_gender_bar = px.bar(gender_distribution, x='Gender', y='Count',
                            labels={'Gender': 'Sex', 'Count': 'Number of Nurses'})
    fig_gender_bar.update_layout(barmode='group', yaxis=dict(tickformat=","))
    st.plotly_chart(fig_gender_bar)

    st.subheader("License Renewals vs  Total Registered")
    fig = px.bar(registered_renewal_df, x='Year', y=['Renewals', 'Registered'],
                  labels={'value': 'Number of Nurses', 'variable': 'Category'},
                  title='Annual Nurse License Renewals vs Registrations')
    fig.update_layout(barmode='group', yaxis=dict(tickformat=","))
    st.plotly_chart(fig, use_container_width=True, height=200)

with column1:
    df_facilities['Latitude'] = df_facilities['Latitude'].str.replace(',', '.', regex=False)
    df_facilities['Longitude'] = df_facilities['Longitude'].str.replace(',', '.', regex=False)

        # Convert to numeric, coercing errors to NaN
    df_facilities['Latitude'] = pd.to_numeric(df_facilities['Latitude'], errors='coerce')
    df_facilities['Longitude'] = pd.to_numeric(df_facilities['Longitude'], errors='coerce')
    df_facilities['Latitude'] = df_facilities['Latitude'].astype(float)
    df_facilities['Longitude'] = df_facilities['Longitude'].astype(float)
    df_facilities['Count'] = df_facilities['Count'].astype(int)
    df_facilities = df_facilities.dropna(subset=['Latitude', 'Longitude'])
    df_facilities = df_facilities.drop_duplicates(subset=['WorkStationID'])
    df_facilities.loc[df_facilities['WorkStationID'] == 34.0, 'Latitude'] = -13.628105
    df_facilities = df_facilities.rename(columns={'Count': 'Number of Nurses'})
    st.subheader("Nurses Distribution Country Wide")
    fig = px.scatter_mapbox(df_facilities, lat="Latitude", lon="Longitude", hover_name="WorkStationName",
                            hover_data=["Number of Nurses"], color="Number of Nurses", size="Number of Nurses", color_continuous_scale=px.colors.cyclical.IceFire, size_max=15, zoom=5)
    fig.update_layout(mapbox_style="open-street-map",
                      mapbox_center={"lat": df_facilities["Latitude"].mean(), "lon": df_facilities["Longitude"].mean()})
    fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
    st.plotly_chart(fig)



# from datetime import datetime
# import streamlit as st
# import pandas as pd
# import plotly.express as px
# import warnings
# import json
# import plotly.graph_objects as go
# import pydeck as pdk
# import folium
# from streamlit_folium import folium_static
#
# warnings.filterwarnings("ignore")
#
# st.set_page_config(page_title="NMCZ Dashboard", page_icon=":bar_chart:", layout="wide")
# st.title(" :bar_chart: Nursing and Midwife Council of Zambia Dashboard")
# st.markdown('<style>div.block-container{padding-top:1rem;padding-bottom:1rem;}</style>', unsafe_allow_html=True)
#
# # Initialize connections
# def init_connections():
#     conn1 = st.connection("gncz_dbms", type="sql", connect_args={
#         "ssl": {"ca": st.secrets["connections"]["gncz_dbms"]["ssl_ca"]}
#     })
#     conn2 = st.connection("zambia_osp", type="sql", connect_args={
#         "ssl": {"ca": st.secrets["connections"]["gncz_dbms"]["ssl_ca"]}
#     })
#     return conn1, conn2
#
# conn1, conn2 = init_connections()
#
# # Load data
# @st.cache_data(ttl=604800)
# def load_data():
#     df0 = conn1.query("select IndexID, DateOfBirth, Gender from gncz_dbms.indextbl")
#     df00 = conn1.query("select IndexID, AssignProgramID, InstitutionID, CourseID, CommenceDate, CompletionDate from gncz_dbms.assignprogramtbl")
#     df01 = conn1.query("select ProvinceID, ProvinceDesc from gncz_dbms.provincetbl")
#     df02 = conn1.query("select DistrictID, DistrictDesc, ProvinceID from gncz_dbms.districttbl")
#     df03 = conn1.query("select WorkStationID, WorkStationName, DistrictID from gncz_dbms.workstationstbl")
#     df04 = conn1.query("select InstitutionID, InstitutionName, DistrictID from gncz_dbms.traininginstitutiontbl")
#     df05 = conn1.query("select CourseID, CourseDesc, CourseDuration from gncz_dbms.coursetbl")
#     df06 = conn1.query("select RegistrationID, WorkStationID, LicenseYear, FromDate, ToDate from gncz_dbms.retentiontbl where VerificationStatus='A'")
#     df07 = conn1.query("select RegistrationID, AssignProgramID, RegistrationDate from gncz_dbms.registrationtbl")
#     df08 = pd.read_csv('mfl.csv', delimiter=';')
#     return df0, df00, df01, df02, df03, df04, df05, df06, df07, df08
#
# df0, df00, df01, df02, df03, df04, df05, df06, df07, df08 = load_data()
#
# # Data preprocessing
# def preprocess_data():
#     df06['ToDate'] = pd.to_datetime(df06['ToDate'])
#     df07['RegistrationDate'] = pd.to_datetime(df07['RegistrationDate'])
#     df = pd.merge(df0, df00, on='IndexID')
#     df = pd.merge(df, df04, on='InstitutionID')
#     df = pd.merge(df, df02, on='DistrictID')
#     df = pd.merge(df, df01, on='ProvinceID')
#     df = pd.merge(df, df05, on='CourseID')
#     return df
#
# df = preprocess_data()
#
# # Filter data by date range
# def filter_by_date(df):
#     df["CommenceDate"] = pd.to_datetime(df["CommenceDate"], errors='coerce')
#     startDate = pd.to_datetime(df["CommenceDate"]).min()
#     endDate = pd.to_datetime(df["CommenceDate"]).max()
#     col1, col2 = st.columns((2))
#     with col1:
#         date1 = pd.to_datetime(st.date_input("Start Date", startDate), errors='coerce')
#     with col2:
#         date2 = pd.to_datetime(st.date_input("End Date", endDate), errors='coerce')
#     return df[(df["CommenceDate"] >= date1) & (df["CommenceDate"] <= date2)].copy()
#
# df = filter_by_date(df)
#
# # Sidebar filters
# def sidebar_filters(df):
#     st.sidebar.header("Select your Filter: ")
#     province = st.sidebar.multiselect("Pick a Province", df["ProvinceDesc"].unique())
#     district = st.sidebar.multiselect("Pick a District", df["DistrictDesc"].unique())
#     program = st.sidebar.multiselect("Pick a Program", df["CourseDesc"].unique())
#     return province, district, program
#
# province, district, program = sidebar_filters(df)
#
# # Apply filters
# def apply_filters(df, province, district, program):
#     if province:
#         df = df[df["ProvinceDesc"].isin(province)]
#     if district:
#         df = df[df["DistrictDesc"].isin(district)]
#     if program:
#         df = df[df["CourseDesc"].isin(program)]
#     return df
#
# filtered_df = apply_filters(df, province, district, program)
#
# # Calculate metrics
# def calculate_metrics(df, df0, df07, df06, df08):
#     df07_merged = pd.merge(df07, df00[['AssignProgramID', 'IndexID']], on='AssignProgramID')
#     total_nurses_filtered = pd.merge(df0, df07_merged, on='IndexID').drop_duplicates(subset='IndexID')
#     total_nurses = total_nurses_filtered['IndexID'].nunique()
#     cleaned_df = pd.merge(df0, total_nurses_filtered, on='IndexID')
#     print(cleaned_df)
#     cleaned_df['Age'] = datetime.now().year - pd.to_datetime(cleaned_df['DateOfBirth_y'], errors='coerce').dt.year
#     average_age = cleaned_df['Age'].mean()
#     total_count = len(cleaned_df)
#     female_count = cleaned_df[cleaned_df['Gender_y'] == 'F'].shape[0]
#     female_percentage = (female_count / total_count) * 100
#     hnp_nurses = (df[df['CourseID'] == 44]['IndexID'].nunique() / total_count) * 100
#     bachelor_nurses = (df[df['CourseDesc'].str.contains('BACHELOR', na=False)]['IndexID'].nunique() / total_count) * 100
#     df_retentions = df06[df06['ToDate'].dt.year == 2024]
#     df_facilities = pd.merge(df_retentions, df08, on='WorkStationID').dropna(subset=['Latitude', 'Longitude'])
#     df_facilities = df_facilities.groupby(['WorkStationID', 'WorkStationName', 'Latitude', 'Longitude']).size().reset_index(name='Count')
#     return total_nurses, average_age, female_percentage, hnp_nurses, bachelor_nurses, df_facilities
#
# total_nurses, average_age, female_percentage, hnp_nurses, bachelor_nurses, df_facilities = calculate_metrics(df, df0, df07, df06, df08)
#
# # Plot metrics
# def plot_metrics(total_nurses, average_age, female_percentage, hnp_nurses, bachelor_nurses):
#     cl1, cl2, cl3, cl4, cl5 = st.columns((5))
#     with cl1:
#         fig_total_nurses = go.Figure(go.Indicator(mode="number", value=total_nurses, number={'valueformat': 'f'}, title={"text": "Total Registered Nurses"}))
#         st.plotly_chart(fig_total_nurses, use_container_width=True)
#     with cl2:
#         fig_average_age = go.Figure(go.Indicator(mode="number", value=average_age, title={"text": "Average Age"}))
#         st.plotly_chart(fig_average_age, use_container_width=True)
#     with cl3:
#         fig_female_percentage = go.Figure(go.Indicator(mode="number", value=female_percentage, title={"text": "Female Percentage"}, number={"suffix": "%"}))
#         st.plotly_chart(fig_female_percentage, use_container_width=True)
#     with cl4:
#         fig_hnp_nurses = go.Figure(go.Indicator(mode="number", value=hnp_nurses, title={"text": "HIV Nurse Practitioners"}, number={"suffix": "%"}))
#         st.plotly_chart(fig_hnp_nurses, use_container_width=True)
#     with cl5:
#         fig_bachelor_nurses = go.Figure(go.Indicator(mode="number", value=bachelor_nurses, title={"text": "Percentage With Bachelors"}, number={"suffix": "%"}))
#         st.plotly_chart(fig_bachelor_nurses, use_container_width=True)
#
# plot_metrics(total_nurses, average_age, female_percentage, hnp_nurses, bachelor_nurses)
#
# # Plot distributions
# def plot_distributions(filtered_df, df2):
#     col1, col2 = st.columns((2))
#     with col1:
#         st.subheader("Number of Nurses Trained per Institution")
#         nursesperschool_df = filtered_df.groupby('InstitutionName').size().reset_index(name='count')
#         fig = px.bar(nursesperschool_df, x='InstitutionName', y='count', labels={'InstitutionName': 'Institution Name', 'count': 'Number of Nurses Trained'})
#         st.plotly_chart(fig, use_container_width=True)
#         with st.expander("Facility ViewData"):
#             st.write(nursesperschool_df.style.background_gradient(cmap="Blues"))
#             csv = nursesperschool_df.to_csv(index=False).encode('utf-8')
#             st.download_button("Download Data", data=csv, file_name="schools.csv", mime="text/csv", help="Click here to download the facility count data")
#         st.subheader("Age Group Distribution")
#         df2['Age'] = datetime.now().year - pd.to_datetime(df2['DateOfBirth'], errors='coerce').dt.year
#         bins = [0, 20, 30, 40, 50, 60, 70, 80, 100]
#         labels = ["0-20", "20-30", "30-40", "40-50", "50-60", "60-70", "70-80", "80+"]
#         df2['AgeGroup'] = pd.cut(df2['Age'], bins=bins, labels=labels, right=False)
#         age_distribution = df2['AgeGroup'].value_counts().reset_index()
#         age_distribution.columns = ['AgeGroup', 'Count']
#         fig_age_pie = px.pie(age_distribution, values='Count', names='AgeGroup', title="Age Distribution", hole=0.5)
#         st.plotly_chart(fig_age_pie)
#         st.subheader("Distribution of Nurses and Midwives by Region")
#         region_distribution = df2['ProvinceDesc'].value_counts().reset_index()
#         region_distribution.columns = ['ProvinceDesc', 'Count']
#         fig_region_bar = px.bar(region_distribution, x='ProvinceDesc', y='Count', title="Provincial Distribution")
#         st.plotly_chart(fig_region_bar)
#     with col2:
#         st.subheader("Nurse Distributions Per Program")
#         coursecount_df = filtered_df.groupby("CourseDesc").size().reset_index(name='Count')
#         fig = px.pie(coursecount_df, values='Count', names='CourseDesc', hole=0.5)
#         st.plotly_chart(fig, use_container_width=True)
#         with st.expander("Programs View Data"):
#             st.write(coursecount_df.style.background_gradient(cmap="Purples"))
#             csv = coursecount_df.to_csv(index=False).encode('utf-8')
#             st.download_button("Download Data", data=csv, file_name="programs.csv", mime="text/csv", help="Click here to download the program count data")
#         st.subheader("Distribution of Nurses and Midwives by Gender")
#         df2['Gender'] = df2['Gender'].map({'M': 'Male', 'F': 'Female'})
#         gender_distribution = df2['Gender'].value_counts().reset_index()
#         gender_distribution.columns = ['Gender', 'Count']
#         fig_gender_pie = px.pie(gender_distribution, values='Count', names='Gender', title="Gender Distribution", hole=0.5)
#         st.plotly_chart(fig_gender_pie)
#         fig_gender_bar = px.bar(gender_distribution, x='Gender', y='Count', title="Gender Distribution")
#         st.plotly_chart(fig_gender_bar)
#
# plot_distributions(filtered_df, df)

# Plot map
# def plot_map(df_facilities):
#     with open('custom.geo.json', encoding='utf-8') as f:
#         county_geojson = json.load(f)
#     view_state = pdk.ViewState(
#         latitude=df_facilities['Latitude'].mean(),
#         longitude=df_facilities['Longitude'].mean(),
#         zoom=5,
#         pitch=0
#     )
#     facility_layer = pdk.Layer(
#         "ScatterplotLayer",
#         data=df_facilities,
#         get_position='[longitude, latitude]',
#         get_fill_color='[0, 0, 255, 160]',
#         get_radius=5000,
#         pickable=True
#     )
#     county_layer = pdk.Layer(
#         "GeoJsonLayer",
#         data=county_geojson,
#         stroked=True,
#         filled=False,
#         get_line_color=[255, 0, 0],
#         line_width_min_pixels=2
#     )
#     mapped = pdk.Deck(
#         layers=[county_layer, facility_layer],
#         initial_view_state=view_state,
#         tooltip={"text": "Facility: {facility_name}\nNurses: {Count}"}
#     )
#     st.pydeck_chart(mapped)
#
# plot_map(df_facilities)

# def plot_map(df_facilities):
#     # Clean the Latitude and Longitude columns
#     df_facilities['Latitude'] = df_facilities['Latitude'].str.replace(',', '.', regex=False)
#     df_facilities['Longitude'] = df_facilities['Longitude'].str.replace(',', '.', regex=False)
#
#     # Convert to numeric, coercing errors to NaN
#     df_facilities['Latitude'] = pd.to_numeric(df_facilities['Latitude'], errors='coerce')
#     df_facilities['Longitude'] = pd.to_numeric(df_facilities['Longitude'], errors='coerce')
#
#     # Drop rows with NaN values in Latitude or Longitude
#     df_facilities = df_facilities.dropna(subset=['Latitude', 'Longitude'])
#
#     with open('custom.geo.json', encoding='utf-8') as f:
#         county_geojson = json.load(f)
#
#     view_state = pdk.ViewState(
#         latitude=df_facilities['Latitude'].mean(),
#         longitude=df_facilities['Longitude'].mean(),
#         zoom=5,
#         pitch=0
#     )
#
#     facility_layer = pdk.Layer(
#         "ScatterplotLayer",
#         data=df_facilities,
#         get_position='[longitude, latitude]',
#         get_fill_color='[0, 0, 255, 160]',
#         get_radius=5000,
#         pickable=True
#     )
#
#     county_layer = pdk.Layer(
#         "GeoJsonLayer",
#         data=county_geojson,
#         stroked=True,
#         filled=False,
#         get_line_color=[255, 0, 0],
#         line_width_min_pixels=2
#     )
#
#     mapped = pdk.Deck(
#         layers=[county_layer, facility_layer],
#         initial_view_state=view_state,
#         tooltip={"text": "Facility: {facility_name}\nNurses: {Count}"}
#     )
#
#     st.pydeck_chart(mapped)


# def plot_map(df_facilities):
#     # Clean the Latitude and Longitude columns
#     df_facilities['Latitude'] = df_facilities['Latitude'].str.replace(',', '.', regex=False)
#     df_facilities['Longitude'] = df_facilities['Longitude'].str.replace(',', '.', regex=False)
#
#     # Convert to numeric, coercing errors to NaN
#     df_facilities['Latitude'] = pd.to_numeric(df_facilities['Latitude'], errors='coerce')
#     df_facilities['Longitude'] = pd.to_numeric(df_facilities['Longitude'], errors='coerce')
#     df_facilities['Latitude'] = df_facilities['Latitude'].astype(float)
#     df_facilities['Longitude'] = df_facilities['Longitude'].astype(float)
#     df_facilities['Count'] = df_facilities['Count'].astype(int)
#     df_facilities = df_facilities.dropna(subset=['Latitude', 'Longitude'])
#     df_facilities = df_facilities.drop_duplicates(subset=['WorkStationID'])
#     df_facilities.loc[df_facilities['WorkStationID'] == 34.0, 'Latitude'] = -13.628105
#
#     # Check for missing, invalid, or non-numeric values in Latitude and Longitude
#     invalid_data = df_facilities[
#         df_facilities['Latitude'].isna() |
#         (df_facilities['Latitude'] < -90) | (df_facilities['Latitude'] > 90) |
#         df_facilities['Longitude'].isna() |
#         (df_facilities['Longitude'] < -180) | (df_facilities['Longitude'] > 180) |
#         ~df_facilities['Latitude'].apply(pd.to_numeric, errors='coerce').notna() |
#         ~df_facilities['Longitude'].apply(pd.to_numeric, errors='coerce').notna()
#         ]
#
#     if not invalid_data.empty:
#         print("Invalid data found in the following rows:")
#         print(invalid_data)
#     else:
#         print("All Latitude and Longitude values are valid.")
#
#     # Debug: Print the first few rows of df_facilities to verify data
#     # print(df_facilities.head())
#
#     # fig = px.scatter_mapbox(df_facilities, lat="Latitude", lon="Longitude", hover_name="WorkStationName", hover_data=["Count"],
#     #                         color="Count", size="Count", color_continuous_scale=px.colors.cyclical.IceFire, size_max=15, zoom=5)
#     # fig.update_layout(mapbox_style="open-street-map")
#     # fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
#     # st.plotly_chart(fig)
#
#     fig = px.scatter_mapbox(df_facilities, lat="Latitude", lon="Longitude", hover_name="WorkStationName", hover_data=["Count"],
#                             color="Count", size="Count", color_continuous_scale=px.colors.cyclical.IceFire,
#                             size_max=15, zoom=5)
#
#     fig.update_layout(mapbox_style="open-street-map")
#     fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
#     st.plotly_chart(fig)
#
#     # with open('custom.geo.json', encoding='utf-8') as f:
#     #     county_geojson = json.load(f)
#     #
#     # m = folium.Map(location=[df_facilities['Latitude'].mean(), df_facilities['Longitude'].mean()], zoom_start=5)
#     #
#     # # Add a marker for each facility with nurses count in the tooltip
#     # for _, row in df_facilities.iterrows():
#     #     tooltip_text = f"{row['WorkStationName']}\nNurses: {row['Count']}"
#     #     folium.Marker(
#     #         [row['Latitude'], row['Longitude']],
#     #         tooltip=tooltip_text
#     #     ).add_to(m)
#     #
#     # # Load and add county boundaries (replace with actual file path)
#     # folium.GeoJson('countries.geojson').add_to(m)
#     #
#     # # Display the map in Streamlit
#     # folium_static(m)
#
# # Call the plot_map function
# plot_map(df_facilities)
