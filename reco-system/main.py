import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from google.cloud import bigquery
import folium
from streamlit_folium import folium_static
from geopy.distance import geodesic

# Configura las credenciales desde las secrets de Streamlit
credentials = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])

# Autenticación con BigQuery
client = bigquery.Client(credentials=credentials)

# Función principal
def main():
    st.title('🚀 Market Mate - Discover the Finest Spots, Seamlessly')
    st.write('***')

    # Variable de estado
    estado = st.session_state.get('estado', 'entrada_datos')

    if estado == 'entrada_datos':
        entrada_datos()
    elif estado == 'buscar_empresas':
        buscar_empresas()
    elif estado == 'mostrar_reviews':
        mostrar_reviews()

def entrada_datos():
    image_path = "reco-system/principal.jpg"
    st.image(image=image_path, use_column_width=True)
    image_path1 = "reco-system/logo.png"
    st.sidebar.image(image=image_path1, use_column_width=True)

    st.sidebar.header('Data Entry')
    st.sidebar.subheader('✅ Select State')
    tables = {'California':'data-ops-mind.California.california_data',
            'Florida':'data-ops-mind.Florida.florida_data', 
            'Illinois':'data-ops-mind.Illinois.illinois_data', 
            'New York':'data-ops-mind.New_York.newyork_data', 
            'Texas':'data-ops-mind.Texas.texas_data'}

    state = st.sidebar.selectbox('States:', ['California','Florida', 'Illinois', 'New York', 'Texas'])

    if state in tables:
        table = tables[state]
    else:
        table = None


    st.sidebar.subheader('✅ Enter your current location')
    my_latitude_str = st.sidebar.text_input('Latitude:', '00.00000')
    my_longitude_str = st.sidebar.text_input('Longitude:', '00.00000')

    my_latitude = float(my_latitude_str)
    my_longitude = float(my_longitude_str)

    st.sidebar.subheader('✅ Select Line')
    rubro = st.sidebar.selectbox('line:', ['Restaurants',
            'Shopping',
            'Health and Beauty',
            'Rental Services',
            'Tourism',
            'Entertainment',
            'Health and Hospitals',
            'Sports',
            'Arts and Crafts',
            'Events and Weddings',
            'Automotive',
            'Education and Learning',
            'Veterinary and Pets',
            'Gardening and Home Services',
            'Technology, Networks, Electronics, and Engineering',
            'Industry',
            'Professional Services',
            'Other'])

    # Modifica la seleccion para que sirva en la consulta:
    rubro = rubro.replace(" ", "_").replace(",", "_")

    # Consulta a BigQuery para obtener las recomendaciones
    query = f"""
        SELECT name, address, avg_rating, latitude, longitude, satisfaction, percent_good_reviews, identificador
        FROM {table}
        WHERE {rubro} = 1   
    """

    # Ejecuta la consulta y carga los resultados en un DataFrame
    df = pd.read_gbq(query, credentials=credentials, project_id="data-ops-mind")

    distancia_maxima = st.sidebar.slider('Select a maximum distance (in kilometers):', 1, 100, 10)

    if st.sidebar.button("Search for nearby businesses 🔍"):
        st.session_state.my_latitude = my_latitude
        st.session_state.my_longitude = my_longitude
        st.session_state.df = df
        st.session_state.distancia_max = distancia_maxima
        st.session_state.table = table
        st.session_state.rubro = rubro
        st.session_state.estado = 'buscar_empresas'

def buscar_empresas():

    # Obtener coordenadas
    my_latitude = st.session_state.my_latitude
    my_longitude = st.session_state.my_longitude
    df = st.session_state.df
    distancia_maxima = st.session_state.distancia_max
    mis_coordenadas = (my_latitude, my_longitude)

    image_path1 = "reco-system/logo.png"
    st.sidebar.image(image=image_path1, use_column_width=True)

    # Sidebar con información
    st.sidebar.title("🗺️ Map of Nearest Businesses")

    # Texto en el sidebar
    st.sidebar.markdown("To the right, recommendations are displayed for the selected parameters.")

    # Título para la información sobre empresas cercanas
    st.sidebar.title("📍 Info about Nearby Businesses")

    # Texto en el sidebar
    st.sidebar.markdown("To the right, a table displays information about nearby businesses.")

    # Texto resaltado para ver reseñas con emojis
    st.sidebar.markdown("📢 If you want to see reviews left by Google Maps and Yelp users for these businesses, select them and press the 'View Reviews' button. 📝")

    if st.sidebar.button('⬅️ Back'):
        st.session_state.estado = 'entrada_datos'

    # Función para calcular la distancia entre dos puntos
    def calcular_distancia(row):
        ubicacion = (row['latitude'], row['longitude'])
        return geodesic(mis_coordenadas, ubicacion).kilometers
   
    # Aplicar la función de cálculo de distancia al DataFrame
    df['Distancia'] = df.apply(calcular_distancia, axis=1)
    # Filtrar registros dentro de la distancia máxima
    registros_cercanos = df[df['Distancia'] <= distancia_maxima]

    #ESTA PARTE GRAFICA
    st.header("🗺️ Map of Nearby Business")
    # Crear un mapa interactivo con folium
    m = folium.Map(location=[my_latitude, my_longitude], zoom_start=15, tiles="Cartodb Positron")
    # Agregar marcadores para los puntos cercanos
    for index, row in registros_cercanos.iterrows():
        folium.Marker([row['latitude'], row['longitude']], tooltip=row[['name','satisfaction']]).add_to(m)
    # Agregar un marcador para tus coordenadas
    icon = folium.Icon(color='red', icon='star')
    folium.Marker([my_latitude, my_longitude], tooltip='Tus Coordenadas', icon=icon).add_to(m)
    # Mostrar el mapa en Streamlit
    folium_static(m)

    # Mostrar los registros cercanos en Streamlit
    st.header(f"📍 Business Info within {distancia_maxima} km of Your Current Location:")
    
    
    # Mostrar el DataFrame con la columna de checkboxes
    registros_cercanos['Select'] = False
    columnas = ['Select','Distancia', 'name', 'address','avg_rating','percent_good_reviews','satisfaction','identificador']
    registros_cercanos['percent_good_reviews'] = (registros_cercanos['percent_good_reviews'] * 100).astype(int).astype(str) + '%'
    registros_cercanos = registros_cercanos[columnas]
    registros_cercanos = registros_cercanos.rename(columns={'Distancia': '📍Distance', 'name': 'Name', 'address':'🌐Address', 'avg_rating':'⭐️Avg.Rating','percent_good_reviews':'👍% Good Reviews'})

    edited = st.data_editor(registros_cercanos)
    # Filtrar el DataFrame para obtener los registros marcados como True en 'Selection'
    registros_seleccionados = edited[edited['Select'] == True]
    # Extraer los valores de la columna 'identificador' de los registros seleccionados y almacenarlos en una lista
    identificadores_seleccionados = registros_seleccionados['identificador'].tolist()

    if st.button('View Reviews'):
        st.session_state.identificador = identificadores_seleccionados
        st.session_state.estado = 'mostrar_reviews'

def mostrar_reviews():
    identificador = st.session_state.identificador
    table = st.session_state.table 
    rubro = st.session_state.rubro 

    identificadores_str = ', '.join([f"'{id}'" for id in identificador])

    image_path1 = "reco-system/logo.png"
    st.sidebar.image(image=image_path1, use_column_width=True)
    # Sidebar title
    st.sidebar.header("Business Reviews")

    # Main text with emojis
    st.sidebar.markdown("""
    To the right, you will find the reviews received for each business. You can find:

    📝 The text of the review.
    ⭐ The rating.
    📅 The date.
    😃 or 😠 The sentiment generated by that review (positive or negative).

    Remember, you can sort the tables by clicking on the headers. For example, if you want to sort only positive reviews, click on the "Opinion" header.
    """)

    # Check if identificadores_str is not empty
    if identificadores_str:
        # Consulta a BigQuery para obtener las recomendaciones
        query2 = f"""
            SELECT name, address, satisfaction, text_list, rating_list, date_list, labels
            FROM `{table}`
            WHERE identificador IN ({identificadores_str}) 
        """
        # Ejecuta la consulta y carga los resultados en un DataFrame
        reviews = pd.read_gbq(query2, credentials=credentials, project_id="data-ops-mind")

        # Función para extraer elementos de la lista JSON interna
        def extract_elements(valor):
            lista_interna = valor['list']
            elementos_internos = [item['element'] for item in lista_interna]
            return elementos_internos

        # Aplicar las funciones a las columnas correspondientes
        reviews['text_list'] = reviews['text_list'].apply(extract_elements)
        reviews['rating_list'] = reviews['rating_list'].apply(extract_elements)
        reviews['date_list'] = reviews['date_list'].apply(extract_elements)
        reviews['labels'] = reviews['labels'].apply(extract_elements)

        for i in range(reviews.shape[0]):
            st.header(f'➡️ Selected Business #{i+1}')
            st.subheader(f'🏪 Business Name: {reviews.name[i]}')
            st.write(f'🌐 Address: {reviews.address[i]}')
            st.write(f'📌 Overall Satisfaction Level: {reviews.satisfaction[i]}')

            lista_texto = []
            lista_rating = []
            lista_fecha = []
            lista_etiqueta = []

            for j in range(len(reviews.date_list[i])):
                if reviews['text_list'][i][j] is not None:
                    lista_texto.append(reviews.text_list[i][j])
                    lista_rating.append(reviews.rating_list[i][j])
                    lista_fecha.append(reviews.date_list[i][j])
                    lista_etiqueta.append(reviews.labels[i][j])
            data = {'📝Review:': lista_texto, '⭐️Rating': lista_rating, '📅Date': lista_fecha, '💬Opinion': lista_etiqueta}
            tablas = pd.DataFrame(data)
            tablas['📅Date'] = pd.to_datetime(tablas['📅Date']).dt.strftime('%Y-%m-%d')
            # Reemplazar valores en la columna 'sentimiento' con emojis
            reemplazos = {'POSITIVE': '😃 POSITIVE', 'NEGATIVE': '😠 NEGATIVE'}
            tablas['💬Opinion'] = tablas['💬Opinion'].replace(reemplazos)

            st.write(tablas)
    else:
        st.subheader("⚠️ You need to choose at least one business ⚠️")


    if st.sidebar.button('⬅️ Back'):
        st.session_state.estado = 'buscar_empresas'

# Ejecutar la función principal
if __name__ == '__main__':
    main()