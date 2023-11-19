import json
import yaml
import time
import boto3
import logging
import requests
import pandas as pd
import streamlit as st
import streamlit_authenticator as stauth

from io import BytesIO
from datetime import datetime
from yaml.loader import SafeLoader

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3', aws_access_key_id=st.secrets["AK"], aws_secret_access_key=st.secrets["SK"], region_name='us-east-1')

api_gateway_client = boto3.client('apigateway', aws_access_key_id=st.secrets["AK"], aws_secret_access_key=st.secrets["SK"], region_name='us-east-1')

# --- User authentication ---

with open('credentials.yaml', 'r') as file:
    config = yaml.load(file, Loader=SafeLoader)

authenticator = stauth.Authenticate(
    config['credentials'], cookie_name="data_annotation", key="randomKeyCookies", cookie_expiry_days=30)

st.session_state.name, st.session_state.authentication_status, st.session_state.username = authenticator.login("Login", "main")

if st.session_state.authentication_status == False:
    st.error('Username/password is incorrect.')

if st.session_state.authentication_status == None:
    st.warning('Please enter your credentials.')

if st.session_state.authentication_status:
    st.write(
        f'Logged in as **{st.session_state.name} - {st.session_state.username}**')
    authenticator.logout('Logout', 'main')

    # --- Import data ---
    url = 'https://ktis60hh3m.execute-api.us-east-1.amazonaws.com/dev/get-unlabeled-data'
    response = api_gateway_client.get_api_key(
        apiKey='as2q9nzhkd',
        includeValue=True
    )
    apikey = response['value']

    with st.spinner(text="Retrieving data for labeling process...", cache=False):
        try:
            r = requests.post(url, json={"username": st.session_state.username}, headers={'x-api-key': apikey})
            df_json = json.loads(r.content)
            df = pd.DataFrame(df_json)
        except Exception as e:
            st.error('An error ocurred retrieving the data.')
            st.error(e)
            df = pd.DataFrame()

    factor_labels = [
        "No Factor",
        "Factor 1: Constraints on Government Powers",
        "Factor 2: Absence of Corruption",
        "Factor 3: Open Government",
        "Factor 4: Fundamental Rights",
        "Factor 5: Order and Security",
        "Factor 6: Regulatory Enforcement",
        "Factor 7: Civil Justice",
        "Factor 8: Criminal Justice",
    ]

    sentiment_labels = [
        "Very positive",
        "Positive",
        "Neutral",
        "Negative",
        "Very negative",
    ]

    if 'idx' not in st.session_state:
        st.session_state.idx = 0

    if 'stage' not in st.session_state:
        st.session_state.stage = 0

    if 'show_second_form' not in st.session_state:
        st.session_state.show_second_form = False

    if 'submitted_second_form' not in st.session_state:
        st.session_state.submitted_second_form = False

    if 'annotations' not in st.session_state:
        st.session_state.annotations = pd.DataFrame(
            {'article_id': [], 'link': [], 'factor(s)': [], 'sentiment': [], 'comments': []})

    if 'available_factor_labels' not in st.session_state:
        st.session_state.available_factor_labels = factor_labels

    if 'available_sentiment_labels' not in st.session_state:
        st.session_state.available_sentiment_labels = sentiment_labels

    @st.cache_data
    def convert_df_to_csv(df):
        # IMPORTANT: Cache the conversion to prevent computation on every rerun
        return df.to_csv().encode('utf-8')

    def show_sample(streamlit_placeholder, sample_idx):
        if sample_idx < len(df):
            with streamlit_placeholder.container():
                st.markdown("""
                <style>
                .title {
                    font-size:30px !important;
                }
                .subtitle {
                    font-size:16px !important;
                }
                .url_btn {
                    background-color:#ECECEC;
                    padding:10px;
                    border-radius:5px;
                    text-decoration:none;
                }
                </style>
                """, unsafe_allow_html=True)
                st.markdown(
                    f"<a style='color:#000000;' class='url_btn' target='_blank' href='{df.iloc[sample_idx]['link']}'>News source</a>", unsafe_allow_html=True)
                st.markdown(
                    f"<b class='subtitle'>{df.iloc[sample_idx]['country'].title()}</b>", unsafe_allow_html=True)
                st.markdown(
                    f"<b class='title'>{df.iloc[sample_idx]['title_eng']}</b>", unsafe_allow_html=True)
                with st.expander("Click here to see the news description"):
                    st.markdown(
                        f"<p class='subtitle'>{df.iloc[sample_idx]['desc_eng']}</p>", unsafe_allow_html=True)
                with st.expander("Click here to see the full news content"):
                    st.markdown(
                        f"<p class='subtitle'>{df.iloc[sample_idx]['content_eng']}</p>", unsafe_allow_html=True)

    def set_label(label, sample_idx):
        if sample_idx < len(df):
            st.session_state.annotations = pd.concat([st.session_state.annotations,
                                                      pd.DataFrame({'article_id': [df.iloc[sample_idx]['article_id']], 
                                                                    'link': [df.iloc[sample_idx]['link']], 
                                                                    'factor(s)': [label[0]], 
                                                                    'sentiment': [label[1]], 
                                                                    'comments': [label[2]]})],
                                                     ignore_index=True)

    def set_stage(stage):
        st.session_state.stage = stage

    def send_data_to_s3(data: pd.DataFrame):

        # process data
        data.drop_duplicates(subset=['article_id'], inplace=True)
        data.dropna(inplace=True)

        # send data to aws s3
        out_buffer = BytesIO()
        data.to_parquet(out_buffer, index=False, compression='gzip')

        try:
            s3_client.put_object(Body=out_buffer.getvalue(),
                                 Bucket='raw-labeled-news-data',
                                 Key=f'{st.session_state.username}/raw_labeled_data_{datetime.now().strftime("%Y%m%d_%H:%M:%S")}.parquet.gzip')
            st.success('The annotated data has been sent. Thank you!')
            time.sleep(3)
            st.stop()
        except Exception as e:
            st.error(e)
            raise
        return True

    first_form = st.empty()
    second_form = st.empty()
    with first_form.form("isrelated"):
        data_container = st.empty()
        show_sample(data_container, st.session_state.idx)

        isrelated = st.radio(
            'Is this news text related to the Rule of Law?', ['Yes', 'No'])
        st.form_submit_button("Submit", on_click=set_stage, args=(1,))

        if st.session_state.stage > 0:
            if isrelated == 'Yes':
                with second_form.form('labeling'):
                    selected_factor_label = st.multiselect(
                        "Select to which factor(s) this news text belongs **(if you select multiple, select them in order of relevance)**:", sorted(st.session_state.available_factor_labels))
                    selected_sentiment_label = st.radio(
                        f"How does this impact the Rule of Law in {df.iloc[st.session_state.idx]['country']} **(based on the most relevant factor)**:", sorted(st.session_state.available_sentiment_labels))

                    comments = st.text_input(label="Comments")
                
                    st.form_submit_button(
                        label="Submit selection of factor and sentiment", on_click=set_stage, args=(2,))

                    if st.session_state.stage > 1:
                        print(st.session_state.stage)
                        set_label(
                            [str(selected_factor_label), selected_sentiment_label, comments], st.session_state.idx)
                        second_form.empty()
                        st.session_state.idx += 1

            elif isrelated == 'No':
                set_label(['Not related', 'Not related', 'Not related'], st.session_state.idx)
                st.session_state.idx += 1

            show_sample(data_container, st.session_state.idx)
            if st.session_state.idx >= len(df):
                data_container.text("No more data to annotate")

            st.session_state.stage = 0

    st.info(f"Annotated: {len(st.session_state.annotations)}")

    if len(st.session_state.annotations) >= 2:
        st.markdown(
            '<p style="font-size:14px;">Once the data is sent, your session will be closed.</p>', unsafe_allow_html=True)
        st.button("Send annotated data to the cloud",
                  on_click=send_data_to_s3, args=(st.session_state.annotations,))
    else:
        st.markdown(
            '<p style="font-size:14px;">You can only send data to the cloud after 15 or more annotated news.</p>', unsafe_allow_html=True)
        st.button("Send annotated data to the cloud", disabled=True)

    # object dataframe: st.session_state.annotations
    with st.expander("Click here to see annotations"):
        st.write(st.session_state.annotations)
