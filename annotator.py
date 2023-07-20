import pandas as pd
import streamlit as st

dataset = pd.read_parquet('master_news_scraped.parquet.gzip')

factor_labels = [
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
    "Positive",
    "Neutral",
    "Negative",
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
    st.session_state.annotations = pd.DataFrame({'url': [], 'factor(s)': [], 'sentiment': []})

if 'available_factor_labels' not in st.session_state:
    st.session_state.available_factor_labels = factor_labels

if 'available_sentiment_labels' not in st.session_state:
    st.session_state.available_sentiment_labels = sentiment_labels
    
@st.cache_data
def convert_df_to_csv(df):
    # IMPORTANT: Cache the conversion to prevent computation on every rerun
    return df.to_csv().encode('utf-8')

def show_sample(streamlit_placeholder, sample_idx):
    if sample_idx < len(dataset):
        with streamlit_placeholder.container():
            st.markdown("""
            <style>
            .title {
                font-size:30px !important;
            }
            .subtitle {
                font-size:18px !important;
            }
            .url_btn {
                background-color:#ECECEC;
                padding:10px;
                border-radius:5px;
                text-decoration:none;
            }
            </style>
            """, unsafe_allow_html=True)
            st.markdown(f"<a style='color:#000000;' class='url_btn' target='_blank' href='{dataset.iloc[sample_idx]['url']}'>News source</a>", unsafe_allow_html=True)
            st.markdown(f"<b class='title'>{dataset.iloc[sample_idx]['country']}</b>", unsafe_allow_html=True)
            st.markdown(f"<b class='title'>{dataset.iloc[sample_idx]['title_eng']}</b>", unsafe_allow_html=True)
            st.markdown(f"<p class='subtitle'>{dataset.iloc[sample_idx]['desc_eng']}</p>", unsafe_allow_html=True)

def set_label(label, sample_idx):
    if sample_idx < len(dataset):
        st.session_state.annotations = pd.concat([st.session_state.annotations, 
                                                  pd.DataFrame({'url': [dataset.iloc[sample_idx]['url']], 'factor': [label[0]], 'sentiment': [label[1]]})], 
                                                 ignore_index=True)
        
def set_stage(stage):
    st.session_state.stage = stage
        
first_form = st.empty()
second_form = st.empty()
with first_form.form("isrelated"):
    data_container = st.empty()
    show_sample(data_container, st.session_state.idx)

    isrelated = st.radio('Is this news text related to the Rule of Law?', ['Yes', 'No'])
    st.form_submit_button("Submit", on_click=set_stage, args=(1,))

    if st.session_state.stage > 0:
        if isrelated == 'Yes':
            with second_form.form('labeling'):
                selected_factor_label = st.multiselect("Select to which factor(s) this news text belongs **(if you select multiple, select them in order of relevance)**:", sorted(st.session_state.available_factor_labels))
                selected_sentiment_label = st.radio("Select change in Rule of Law:", sorted(st.session_state.available_sentiment_labels))
                st.form_submit_button(label="Submit selection of factor and sentiment", on_click=set_stage, args=(2,))

                if st.session_state.stage > 1:
                    print(st.session_state.stage)
                    set_label([str(selected_factor_label), selected_sentiment_label], st.session_state.idx)
                    second_form.empty()
                    st.session_state.idx += 1

        elif isrelated == 'No':
            set_label(['Not related', 'Not related'], st.session_state.idx)
            st.session_state.idx += 1

        show_sample(data_container, st.session_state.idx)
        if st.session_state.idx >= len(dataset):
            data_container.text("No more data to annotate")
    
        st.session_state.stage = 0


st.info(f"Annotated: {len(st.session_state.annotations)}")

st.download_button(
        "Download annotations as CSV file",
        data=convert_df_to_csv(st.session_state.annotations),
        file_name='annotations.csv',
        mime='text/csv',
    )

with st.expander("Click here to see annotations"):
    st.write(st.session_state.annotations)