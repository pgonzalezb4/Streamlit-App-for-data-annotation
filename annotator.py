import pandas as pd
import streamlit as st

dataset = pd.read_parquet('master_news_scraped.parquet.gzip')

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
    "Positive",
    "Neutral",
    "Negative",
    "No Factor"
]

if 'idx' not in st.session_state:
    st.session_state.idx = 0

if 'annotations' not in st.session_state:
    st.session_state.annotations = pd.DataFrame({'url': [], 'factor': [], 'sentiment': []})

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
            st.markdown(f"Country: `{dataset.iloc[sample_idx]['country']}`")
            st.markdown(f"Source: `{dataset.iloc[sample_idx]['source']}`")
            st.markdown(f"URL: `{dataset.iloc[sample_idx]['url']}`")
            st.markdown(f"News title: `{dataset.iloc[sample_idx]['title_eng']}`")
            st.markdown(f"News summary: `{dataset.iloc[sample_idx]['desc_eng']}`")

def set_label(label, sample_idx):
    if sample_idx < len(dataset):
        st.session_state.annotations = pd.concat([st.session_state.annotations, 
                                                  pd.DataFrame({'url': [dataset.iloc[sample_idx]['url']], 'factor': [label[0]], 'sentiment': [label[1]]})], 
                                                 ignore_index=True)

with st.form('labeling', clear_on_submit=True):

    data_container = st.empty()

    show_sample(data_container, st.session_state.idx)
    selected_factor_label = st.radio("Select to which factor this news text belongs:", sorted(st.session_state.available_factor_labels))
    selected_sentiment_label = st.radio("Select change in Rule of Law:", sorted(st.session_state.available_sentiment_labels))

    submit_col = st.columns(1)
    submit = submit_col[0].form_submit_button("Submit")

    if submit:
        set_label([selected_factor_label, selected_sentiment_label], st.session_state.idx)
        st.session_state.idx += 1
        show_sample(data_container, st.session_state.idx)
        if st.session_state.idx >= len(dataset):
            data_container.text("No more data to annotate")

st.info(f"Annotated: {len(st.session_state.annotations)}")
st.download_button(
    "Download annotations as CSV file",
    data=convert_df_to_csv(st.session_state.annotations),
    file_name='annotations.csv',
    mime='text/csv',
)
st.write(st.session_state.annotations)