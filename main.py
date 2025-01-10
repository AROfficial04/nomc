from flask import Flask, render_template, request, jsonify, send_from_directory
import pandas as pd
import os
from werkzeug.utils import secure_filename


app = Flask(__name__, static_folder='assets')


# Set upload folder and allowed file extensions
UPLOAD_FOLDER = 'uploads'
ALLOWED_EXTENSIONS = {'xls', 'xlsx'}

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

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
            file.save(filepath)

            # Read the Excel file and extract columns
            df = pd.read_excel(filepath)
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

        # Save uploaded files
        file1_path = os.path.join(app.config['UPLOAD_FOLDER'], secure_filename(file1.filename))
        file2_path = os.path.join(app.config['UPLOAD_FOLDER'], secure_filename(file2.filename))
        file1.save(file1_path)
        file2.save(file2_path)

        # Read Excel files and normalize column names
        df_wfm = pd.read_excel(file1_path)
        df_hes = pd.read_excel(file2_path)
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

        # Save processed data to new files
        timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        non_comm_file = os.path.join(app.config['UPLOAD_FOLDER'], f"Non_Comm_{timestamp}.xlsx")
        never_comm_file = os.path.join(app.config['UPLOAD_FOLDER'], f"Never_Comm_{timestamp}.xlsx")
        unmapped_file = os.path.join(app.config['UPLOAD_FOLDER'], f"Unmapped_{timestamp}.xlsx")

        # Save results (include all columns)
        non_comm_data.to_excel(non_comm_file, index=False)
        never_comm_data.to_excel(never_comm_file, index=False)
        unmapped_data.to_excel(unmapped_file, index=False)

        # Prepare preview data (e.g., first 5 rows of the Non-Comm data)
        preview_data = non_comm_data.head(5).to_dict(orient='records')

        summary = {
          "WFM Total Entries": len(df_wfm),  # Total entries in the WFM file
          "HES Total Entries": len(df_hes),  # Total entries in the HES file
          "Non-Comm Count": len(non_comm_data),
          "Never-Comm Count": len(never_comm_data),
          "Unmapped Count": len(unmapped_data),
          "Matched Entries": len(df_wfm[df_wfm[wfm_column].isin(df_hes[hes_column])])  # Entries that match between WFM and HES
        }
        

        return jsonify({
    "nonCommFile": f"/download/{os.path.basename(non_comm_file)}",
    "neverCommFile": f"/download/{os.path.basename(never_comm_file)}",
    "unmappedFile": f"/download/{os.path.basename(unmapped_file)}",
    "previewData": preview_data,  # Optional: Add preview of processed data
    "summary": summary  # Include the summary in the response
})

    except Exception as e:
        return jsonify({"error": str(e)}), 500



# Route to download the processed files
@app.route('/download/<filename>')
def download_file(filename):
    return send_from_directory(app.config['UPLOAD_FOLDER'], filename)




if __name__ == "__main__":
    # Create upload folder if it doesn't exist
    if not os.path.exists(UPLOAD_FOLDER):
        os.makedirs(UPLOAD_FOLDER)

    # Run the app
    app.run()
