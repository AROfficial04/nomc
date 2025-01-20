from flask import Flask, render_template, request, jsonify, send_from_directory
import pandas as pd
import os
from werkzeug.utils import secure_filename
from pymongo import MongoClient
import io

app = Flask(__name__, static_folder='assets')

# Set upload folder and allowed file extensions
UPLOAD_FOLDER = 'uploads'
ALLOWED_EXTENSIONS = {'xls', 'xlsx'}

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# MongoDB connection
client = MongoClient("mongodb+srv://mass:ayamass@nomc.r8hka.mongodb.net/")
db = client['nomc']
collection = db['processed_data']

# Helper function to check file extensions
def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

# Route to render the index.html file
@app.route("/")
def index():
    return render_template("index.html")

# Route to handle file uploads and get columns
@app.route("/get_columns", methods=["POST"])
def get_columns():
    try:
        file = request.files['file']
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)

            # Save the file in memory and read it into a DataFrame
            in_memory_file = io.BytesIO(file.read())
            df = pd.read_excel(in_memory_file)
            columns = df.columns.tolist()

            return jsonify({"columns": columns})
        else:
            return jsonify({"error": "Invalid file format"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Route to process the uploaded data
@app.route("/process_data", methods=["POST"])
def process_data():
    try:
        # Retrieve uploaded files and selected column names
        file1 = request.files['file1']
        file2 = request.files['file2']
        wfm_column = request.form['wfmColumn'].strip()
        hes_column = request.form['hesColumn'].strip()
        non_comm_column = request.form['nonCommColumn'].strip()

        # Read Excel files from memory
        file1_in_memory = io.BytesIO(file1.read())
        file2_in_memory = io.BytesIO(file2.read())
        
        # Read the Excel files into DataFrames
        df_wfm = pd.read_excel(file1_in_memory)
        df_hes = pd.read_excel(file2_in_memory)
        
        # Normalize column names
        df_wfm.columns = df_wfm.columns.str.strip()
        df_hes.columns = df_hes.columns.str.strip()

        # Validate selected columns
        if wfm_column not in df_wfm.columns:
            return jsonify({"error": f"'{wfm_column}' not found in WFM file"}), 400
        if hes_column not in df_hes.columns:
            return jsonify({"error": f"'{hes_column}' not found in HES file"}), 400
        if non_comm_column not in df_hes.columns:
            return jsonify({"error": f"'{non_comm_column}' not found in HES file"}), 400

        # **Non-Comm Logic**: Filter rows where `non_comm_column` has values > 3
        non_comm_data = df_hes[df_hes[non_comm_column].astype(str).apply(lambda x: int(x) > 3 if x.isdigit() else False)]

        # **Never Comm Logic**: Rows in WFM where `wfm_column` is NOT in HES's `hes_column`
        never_comm_data = df_wfm[~df_wfm[wfm_column].isin(df_hes[hes_column])]

        # **Unmapped Logic**: Rows in HES where `hes_column` is NOT in WFM's `wfm_column`
        unmapped_data = df_hes[~df_hes[hes_column].isin(df_wfm[wfm_column])]

        # Store the processed data in MongoDB
        timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        
        # Create document for non_comm_data, never_comm_data, and unmapped_data
        non_comm_data_dict = non_comm_data.to_dict(orient="records")
        never_comm_data_dict = never_comm_data.to_dict(orient="records")
        unmapped_data_dict = unmapped_data.to_dict(orient="records")

        # Insert the data into MongoDB
        collection.insert_many([
            {"type": "Non-Comm", "timestamp": timestamp, "data": non_comm_data_dict},
            {"type": "Never-Comm", "timestamp": timestamp, "data": never_comm_data_dict},
            {"type": "Unmapped", "timestamp": timestamp, "data": unmapped_data_dict}
        ])

        # Prepare summaries for detailed analysis
        def analyze_column(dataframe, columns):
            analysis = {}
            for column in columns:
                if column in dataframe.columns:
                    value_counts = dataframe[column].value_counts().to_dict()
                    analysis[column] = {
                        "unique_count": len(value_counts),
                        "frequencies": value_counts
                    }
            return analysis

        non_comm_analysis = analyze_column(non_comm_data, ["CTWC", "MeterType"])
        never_comm_analysis = analyze_column(never_comm_data, ["Region Name"])
        unmapped_analysis = analyze_column(unmapped_data, ["CTWC", "MeterType"])

        # Prepare overall summary
        summary = {
            "WFM Total Entries": len(df_wfm),
            "HES Total Entries": len(df_hes),
            "Non-Comm Count": len(non_comm_data),
            "Never-Comm Count": len(never_comm_data),
            "Unmapped Count": len(unmapped_data),
            "Matched Entries": len(df_wfm[df_wfm[wfm_column].isin(df_hes[hes_column])]),
            "Detailed Analysis": {
                "Non-Comm": non_comm_analysis,
                "Never-Comm": never_comm_analysis,
                "Unmapped": unmapped_analysis
            }
        }

        return jsonify({
            "summary": summary
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Route to download the processed files (fetch from MongoDB)
@app.route('/download/<data_type>/<timestamp>')
def download_file(data_type, timestamp):
    try:
        # Fetch the data from MongoDB by type and timestamp
        result = collection.find_one({"type": data_type, "timestamp": timestamp})
        if result:
            # If data is found, return it as JSON
            return jsonify(result['data'])
        else:
            return jsonify({"error": "Data not found"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    # Create upload folder if it doesn't exist
    if not os.path.exists(UPLOAD_FOLDER):
        os.makedirs(UPLOAD_FOLDER)

    # Run the app
    app.run(host="0.0.0.0", port=5000, debug=True)
