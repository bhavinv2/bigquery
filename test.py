import streamlit as st
import logging
import json
from google.cloud import bigquery
import vertexai
from vertexai.preview.generative_models import GenerativeModel, HarmCategory, HarmBlockThreshold

logging.basicConfig(level=logging.INFO)

project_id = "numeric-ion-425514-k6"
dataset_id = "bigquery-public-data.world_bank_health_population"
table_id = "health_nutrition_population"


def get_bigquery_description(project_id, dataset_id, table_id):
    try:
        QUERY = f'SELECT * FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.TABLES` WHERE table_name = "{table_id}"'
        logging.info(f"Executing BigQuery description query: {QUERY}")
        client = bigquery.Client(project=project_id)
        query_job = client.query(QUERY)
        rows = query_job.result()
        for row in rows:
            logging.info(f"Fetched table description: {row}")
            return row
        logging.warning("No rows found for table description.")
        return None  # If no rows found
    except Exception as e:
        logging.error(f"Error fetching table description: {e}")
        return None


def execute_query(query):
    try:
        logging.info(f"Executing SQL query: {query}")
        client = bigquery.Client(project=project_id)
        query_job = client.query(query)
        rows = query_job.result()
        logging.info("Query executed successfully.")
        return rows
    except Exception as e:
        logging.error(f"Error executing query: {e}")
        return None


def generate_sql_query(description, question, dataset_id, table_id):
    vertexai.init(project=project_id, location="us-central1")
    model = GenerativeModel(
        model_name="gemini-1.0-pro-002",
        system_instruction=f"""
            You are a BigQuery SQL Expert designed to generate SQL Queries for a given user question.
            Here is the table description for the table:
            {description}.
            Your Response should be in a JSON format. For example:
            {{
                "query": "SELECT * FROM `{project_id}.{dataset_id}.{table_id}`",
                "description": "This query will return all the records from the table {table_id}"
            }}
        """
    )

    generation_config = {
        "max_output_tokens": 2048,
        "temperature": 1,
        "top_p": 1
    }

    safety_settings = {
        HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
        HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
        HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
        HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE
    }

    try:
        logging.info("Generating SQL query using Gemini API...")
        responses = model.generate_content(
            [question],
            generation_config=generation_config,
            safety_settings=safety_settings,
            stream=False
        )

        response = responses[0].text.replace("**json", "").replace("=-*", "")
        logging.info(f"Generated response: {response}")
        response = json.loads(response)
        query = response["query"]
        logging.info(f"Generated SQL query: {query}")
        return query
    except Exception as e:
        logging.error(f"Error generating SQL query: {e}")
        return None


def generate_answer(question, answer):
    vertexai.init(project=project_id, location="us-central1")
    model = GenerativeModel(
        "gemini-1.5-flash-001",
    )

    generation_config = {
        "max_output_tokens": 8192,
        "temperature": 1,
        "top_p": 0.95,
    }

    safety_settings = {
        HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
        HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
        HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
        HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
    }

    try:
        logging.info("Generating answer using Gemini API...")
        responses = model.generate_content(
            [f"User Question: {question} \n\nBigQuery Results: {answer}\n\nPlease summarize the results."],
            generation_config=generation_config,
            safety_settings=safety_settings,
            stream=True,
        )

        for response in responses:
            st.text(response.text)
    except Exception as e:
        logging.error(f"Error generating answer: {e}")


def main():
    st.title("BigQuery Query Generator and Answer Generator")

    # Input fields
    question = st.text_input("Enter your SQL query question:")
    submit_button = st.button("Generate Query and Answer")

    if submit_button:
        logging.info("Submit button clicked.")
        # Get table description
        description = get_bigquery_description(project_id, dataset_id, table_id)

        if description:
            logging.info("Table description fetched successfully.")
            # Generate SQL query
            query = generate_sql_query(description, question, dataset_id, table_id)

            if query:
                logging.info("SQL query generated successfully.")
                # Execute SQL query
                rows = execute_query(query)

                if rows:
                    logging.info("SQL query executed successfully.")
                    # Generate answer
                    generate_answer(question, rows)
                else:
                    st.error("Failed to execute SQL query. Please check your input.")
            else:
                st.error("Failed to generate SQL query. Please try again.")
        else:
            st.error("Failed to fetch table description. Please check your input.")


if __name__ == "__main__":
    main()


def generate():
    vertexai.init(project=project_id, location="us-central1")
    model = GenerativeModel(
        "gemini-1.5-flash-001",
    )

    generation_config = {
        "max_output_tokens": 8192,
        "temperature": 1,
        "top_p": 0.95,
    }

    safety_settings = {
        HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
        HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
        HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
        HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
    }

    try:
        logging.info("Generating additional content using Gemini API...")
        responses = model.generate_content(
            ["Generate some example content"],  # Provide some example content to avoid empty input error
            generation_config=generation_config,
            safety_settings=safety_settings,
            stream=True,
        )

        for response in responses:
            print(response.text, end="")
    except Exception as e:
        logging.error(f"Error generating content: {e}")



generate()
