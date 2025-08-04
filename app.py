import os
import pandas as pd
from flask import Flask, render_template, request, redirect, send_file, url_for, flash, jsonify
from werkzeug.utils import secure_filename
from models import db, Query
import yaml
from urllib.parse import quote_plus
from io import BytesIO
from datetime import datetime


config_path = "config/db_config.yaml"

# Load DB config from YAML
with open(config_path, "r") as f:
    db_config = yaml.safe_load(f)

pg = db_config['postgres']
user = pg['user']
password = pg['password']
password = quote_plus(pg["password"])  # ✅ Encode special chars
host = pg['host']
port = pg['port']
database = pg['database']

app = Flask(__name__)
app.config['SECRET_KEY'] = 'b8f7e2c4-1a3d-4e6b-9f2e-7c5d8a1b2c3e'
app.config['SQLALCHEMY_DATABASE_URI'] = f"postgresql://{user}:{password}@{host}:{port}/{database}"
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = True
app.config['UPLOAD_FOLDER'] = 'uploads'


# Ensure upload folder exists
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

# Initialize database
db.init_app(app)

# File processing status tracking
processing_status = {}

# Allowed file extensions
ALLOWED_EXTENSIONS = {'xlsx', 'xls', 'csv'}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def process_excel_file(filepath):
    try:
        if filepath.endswith('.csv'):
            df = pd.read_csv(filepath, dayfirst=True)
        else:
            df = pd.read_excel(filepath, dayfirst=True)
        required_columns = [
            'QueryID', 'GHAID', 'ScrnID', 'MomID', 'PregID', 'InfantID', 
            'VisitType', 'VisitDate', 'Form', 'Variable_Name', 'Variable_Value',
            'EditType', 'UploadDate', 'FieldType', 'DateEditReported', 
            'Form_Edit_Type', 'VarFormEdit', 'RemoveEdit', 'Notes'
        ]
        
        # Check if all required columns exist
        if not all(col in df.columns for col in required_columns):
            missing_cols = [col for col in required_columns if col not in df.columns]
            return False, f"Missing required columns in Excel file: {', '.join(missing_cols)}"
        
        # Process each row
        for _, row in df.iterrows():
            existing_query = Query.query.filter_by(QueryID=row['QueryID']).first()
            if existing_query:
                # Update existing query
                existing_query.GHAID = row['GHAID'] if pd.notnull(row['GHAID']) else None
                existing_query.ScrnID = row['ScrnID'] if pd.notnull(row['ScrnID']) else None
                existing_query.MomID = row['MomID'] if pd.notnull(row['MomID']) else None
                existing_query.PregID = row['PregID'] if pd.notnull(row['PregID']) else None
                existing_query.InfantID = row['InfantID'] if pd.notnull(row['InfantID']) else None
                existing_query.VisitType = row['VisitType'] if pd.notnull(row['VisitType']) else None
                existing_query.VisitDate = pd.to_datetime(row['VisitDate']).date() if pd.notnull(row['VisitDate']) else None
                existing_query.Form = row['Form'] if pd.notnull(row['Form']) else None
                existing_query.Variable_Name = row['Variable_Name'] if pd.notnull(row['Variable_Name']) else None
                existing_query.Variable_Value = row['Variable_Value'] if pd.notnull(row['Variable_Value']) else None
                existing_query.EditType = row['EditType'] if pd.notnull(row['EditType']) else None
                existing_query.UploadDate = pd.to_datetime(row['UploadDate']).date() if pd.notnull(row['UploadDate']) else None
                existing_query.FieldType = row['FieldType'] if pd.notnull(row['FieldType']) else None
                existing_query.DateEditReported = pd.to_datetime(row['DateEditReported']).date() if pd.notnull(row['DateEditReported']) else None
                existing_query.Form_Edit_Type = row['Form_Edit_Type'] if pd.notnull(row['Form_Edit_Type']) else None
                existing_query.VarFormEdit = row['VarFormEdit'] if pd.notnull(row['VarFormEdit']) else None
                existing_query.RemoveEdit = row['RemoveEdit'] if pd.notnull(row['RemoveEdit']) else None
                existing_query.Notes = row['Notes'] if pd.notnull(row['Notes']) else None
                # Preserve status if it exists
            else:
                # Create new query
                new_query = Query(
                    QueryID=row['QueryID'],
                    GHAID=row['GHAID'] if pd.notnull(row['GHAID']) else None,
                    ScrnID=row['ScrnID'] if pd.notnull(row['ScrnID']) else None,
                    MomID=row['MomID'] if pd.notnull(row['MomID']) else None,
                    PregID=row['PregID'] if pd.notnull(row['PregID']) else None,
                    InfantID=row['InfantID'] if pd.notnull(row['InfantID']) else None,
                    VisitType=row['VisitType'] if pd.notnull(row['VisitType']) else None,
                    VisitDate=pd.to_datetime(row['VisitDate']).date() if pd.notnull(row['VisitDate']) else None,
                    Form=row['Form'] if pd.notnull(row['Form']) else None,
                    Variable_Name=row['Variable_Name'] if pd.notnull(row['Variable_Name']) else None,
                    Variable_Value=row['Variable_Value'] if pd.notnull(row['Variable_Value']) else None,
                    EditType=row['EditType'] if pd.notnull(row['EditType']) else None,
                    UploadDate=pd.to_datetime(row['UploadDate']).date() if pd.notnull(row['UploadDate']) else None,
                    FieldType=row['FieldType'] if pd.notnull(row['FieldType']) else None,
                    DateEditReported=pd.to_datetime(row['DateEditReported']).date() if pd.notnull(row['DateEditReported']) else None,
                    Form_Edit_Type=row['Form_Edit_Type'] if pd.notnull(row['Form_Edit_Type']) else None,
                    VarFormEdit=row['VarFormEdit'] if pd.notnull(row['VarFormEdit']) else None,
                    RemoveEdit=row['RemoveEdit'] if pd.notnull(row['RemoveEdit']) else None,
                    Notes=row['Notes'] if pd.notnull(row['Notes']) else None
                )
                db.session.add(new_query)
        
        db.session.commit()
        return True, "File processed successfully"
    except Exception as e:
        db.session.rollback()
        return False, str(e)

@app.route('/')
def index():
    return redirect(url_for('upload_file'))

@app.route('/upload', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        if 'file' not in request.files:
            flash('No file selected')
            return redirect(request.url)
        
        file = request.files['file']
        if file.filename == '':
            flash('No file selected')
            return redirect(request.url)
        
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(filepath)
            
            success, message = process_excel_file(filepath)
            if success:
                flash('File uploaded and processed successfully')
                return redirect(url_for('view_queries'))
            else:
                flash(f'Error processing file: {message}')
        else:
            flash('Invalid file type. Please upload an Excel file (.xlsx or .xls)')
    
    return render_template('upload.html')

@app.route('/queries')
def view_queries():
    # Get distinct values for filters
    edit_types = db.session.query(Query.EditType).distinct().all()
    edit_types = [et[0] for et in edit_types if et[0]]
    
    visit_types = db.session.query(Query.VisitType).distinct().all()
    visit_types = [vt[0] for vt in visit_types if vt[0]]
    
    forms = db.session.query(Query.Form).distinct().all()
    forms = [f[0] for f in forms if f[0]]
    
    statuses = ['Pending', 'In Progress', 'Resolved', 'Closed']
    
    return render_template('queries.html', 
                         edit_types=edit_types, 
                         visit_types=visit_types, 
                         forms=forms, 
                         statuses=statuses)

@app.route('/api/queries')
def api_queries():
    edit_type = request.args.get('edit_type')
    visit_type = request.args.get('visit_type')
    form = request.args.get('form')
    status = request.args.get('status')
    search = request.args.get('search')
    page = request.args.get('page', 1, type=int)
    
    query_obj = Query.query
    if edit_type:
        query_obj = query_obj.filter(Query.EditType == edit_type)
    if visit_type:
        query_obj = query_obj.filter(Query.VisitType == visit_type)
    if form:
        query_obj = query_obj.filter(Query.Form == form)
    if status:
        query_obj = query_obj.filter(Query.status == status)
    if search:
        search_filter = f"%{search}%"
        query_obj = query_obj.filter(
            db.or_(
                Query.QueryID.ilike(search_filter),
                Query.PregID.ilike(search_filter),
                Query.Notes.ilike(search_filter),
                Query.GHAID.ilike(search_filter)
            )
        )
    
    queries = query_obj.paginate(page=page, per_page=20)
    return jsonify({
        'queries': [q.to_dict() for q in queries.items],
        'has_next': queries.has_next,
        'has_prev': queries.has_prev,
        'page': queries.page,
        'pages': queries.pages
    })

@app.route('/api/query/<int:query_id>', methods=['PUT'])
def update_query(query_id):
    query = Query.query.get_or_404(query_id)
    data = request.get_json()
    
    try:
        # Update only the fields that are provided
        if 'Notes' in data:
            query.Notes = data['Notes']
        if 'status' in data:
            query.status = data['status']
        # if 'EditType' in data:
        #     query.EditType = data['EditType']
        if 'RemoveEdit' in data:
            query.RemoveEdit = data['RemoveEdit']
        
        query.updated_at = datetime.utcnow()
        
        db.session.commit()
        return jsonify({'success': True, 'message': 'Query updated successfully'})
    except Exception as e:
        db.session.rollback()
        return jsonify({'success': False, 'message': str(e)}), 400

# BULK OPERATIONS ENDPOINTS

@app.route('/api/bulk/status', methods=['PUT'])
def bulk_update_status():
    data = request.get_json()
    query_ids = data.get('query_ids', [])
    new_status = data.get('status')
    
    if not query_ids or not new_status:
        return jsonify({'success': False, 'message': 'Missing query IDs or status'}), 400
    
    try:
        Query.query.filter(Query.id.in_(query_ids)).update(
            {Query.status: new_status},
            synchronize_session=False
        )
        db.session.commit()
        return jsonify({'success': True, 'message': f'Updated {len(query_ids)} queries to {new_status}'})
    except Exception as e:
        db.session.rollback()
        return jsonify({'success': False, 'message': str(e)}), 400

@app.route('/api/bulk/edit', methods=['PUT'])
def bulk_edit_queries():
    data = request.get_json()
    query_ids = data.get('query_ids', [])
    updates = data.get('updates', {})
    
    if not query_ids or not updates:
        return jsonify({'success': False, 'message': 'Missing query IDs or updates'}), 400
    
    try:
        Query.query.filter(Query.id.in_(query_ids)).update(
            updates,
            synchronize_session=False
        )
        db.session.commit()
        return jsonify({'success': True, 'message': f'Updated {len(query_ids)} queries'})
    except Exception as e:
        db.session.rollback()
        return jsonify({'success': False, 'message': str(e)}), 400

@app.route('/api/bulk/delete', methods=['DELETE'])
def bulk_delete_queries():
    data = request.get_json()
    query_ids = data.get('query_ids', [])
    
    if not query_ids:
        return jsonify({'success': False, 'message': 'Missing query IDs'}), 400
    
    try:
        deleted_count = Query.query.filter(Query.id.in_(query_ids)).delete(synchronize_session=False)
        db.session.commit()
        return jsonify({'success': True, 'message': f'Deleted {deleted_count} queries'})
    except Exception as e:
        db.session.rollback()
        return jsonify({'success': False, 'message': str(e)}), 400

@app.route('/api/bulk/export')
def bulk_export_queries():
    query_ids = request.args.getlist('query_ids')
    
    if not query_ids:
        return jsonify({'success': False, 'message': 'No queries selected'}), 400
    
    try:
        queries = Query.query.filter(Query.id.in_(query_ids)).all()
        
        # Create DataFrame
        data = []
        for query in queries:
            data.append({
                'QueryID': query.QueryID,
                'GHAID': query.GHAID,
                'ScrnID': query.ScrnID,
                'MomID': query.MomID,
                'PregID': query.PregID,
                'InfantID': query.InfantID,
                'VisitType': query.VisitType,
                'VisitDate': query.VisitDate,
                'Form': query.Form,
                'Variable_Name': query.Variable_Name,
                'Variable_Value': query.Variable_Value,
                'EditType': query.EditType,
                'UploadDate': query.UploadDate,
                'FieldType': query.FieldType,
                'DateEditReported': query.DateEditReported,
                'Form_Edit_Type': query.Form_Edit_Type,
                'VarFormEdit': query.VarFormEdit,
                'RemoveEdit': query.RemoveEdit,
                'Notes': query.Notes,
                'status': query.status
            })
        
        df = pd.DataFrame(data)
        
        # Create Excel file in memory
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Queries')
        output.seek(0)
        
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name='exported_queries.xlsx'
        )
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 400

if __name__ == '__main__':
    with app.app_context():
        db.create_all()
    app.run(debug=True, host='0.0.0.0')