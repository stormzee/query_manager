import os
import pandas as pd
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify, send_file
from werkzeug.utils import secure_filename
from models import db, Query
import yaml
from urllib.parse import quote_plus
from io import BytesIO
from datetime import datetime
import re
from sqlalchemy import text

# Load DB config from YAML
config_path = "config/db_config.yaml"

# Load DB config from YAML
with open(config_path, "r") as f:
    db_config = yaml.safe_load(f)

pg = db_config['postgres']
user = pg['user']
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

# Forms that DO NOT have type_visit column (these use only pregid for queries)
FORMS_WITHOUT_TYPE_VISIT = {
    'mnh11', 'mnh09', 'mnh03', 'mnh00', 'mnh10', 'mnh16', 'mnh17', 'mnh18'
}

# Forms that HAVE type_visit column (these use pregid AND type_visit for queries)
FORMS_WITH_TYPE_VISIT = {
    'mnh01', 'mnh04', 'mnh05', 'mnh06', 'mnh07', 'mnh08', 
    'mnh12', 'mnh13', 'mnh14', 'mnh15', 'mnh25', 'mnh26'
}

# Allowed file extensions
ALLOWED_EXTENSIONS = {'xlsx', 'xls'}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def process_excel_file(filepath):
    """Process Excel file synchronously"""
    try:
        df = pd.read_excel(filepath)
        required_columns = [
            'QueryID', 'GHAID', 'ScrnID', 'MomID', 'PregID', 'InfantID', 
            'VisitType', 'VisitDate', 'Form', 'Variable_Name', 'Variable_Value',
            'EditType', 'UploadDate', 'FieldType', 'DateEditReported', 
            'Form_Edit_Type', 'VarFormEdit', 'RemoveEdit', 'Notes'
        ]
        
        # Check if all required columns exist
        missing_cols = [col for col in required_columns if col not in df.columns]
        if missing_cols:
            return False, f"Missing required columns: {', '.join(missing_cols)}"
        
        # Process each row
        for index, row in df.iterrows():
            try:
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
                
            except Exception as row_error:
                print(f"Error processing row {index}: {str(row_error)}")
                continue
        
        db.session.commit()
        return True, f"Successfully processed {len(df)} rows"
        
    except Exception as e:
        db.session.rollback()
        return False, str(e)

def get_current_value_from_form(form_name, visit_type, pregid, variable_name):
    """Fetch current value using your exact logic"""
    try:
        # Sanitize inputs to prevent SQL injection
        if not re.match(r'^[a-zA-Z0-9_]+$', form_name) or not re.match(r'^[a-zA-Z0-9_]+$', variable_name):
            print(f"Invalid form name or variable name: {form_name}, {variable_name}")
            return None
            
        # Convert form name to lowercase for database queries
        form_name_lower = form_name.lower()
        variable_name_lower = variable_name.lower()
        
        print(f"Debug - Looking for form: {form_name_lower}, variable: {variable_name_lower}, pregid: {pregid}, visit_type: {visit_type}")
        
        # Check if table exists
        table_exists_query = text("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = :table_name
            );
        """)
        
        result = db.session.execute(table_exists_query, {'table_name': form_name_lower}).fetchone()
        print(f"Table exists result: {result}")
        if not result or not result[0]:
            print(f"Table {form_name_lower} does not exist")
            return None
            
        # Check if variable column exists
        column_exists_query = text("""
            SELECT EXISTS (
                SELECT FROM information_schema.columns 
                WHERE table_name = :table_name 
                AND column_name = :column_name
            );
        """)
        
        result = db.session.execute(column_exists_query, {
            'table_name': form_name_lower, 
            'column_name': variable_name_lower
        }).fetchone()
        print(f"Column exists result: {result}")
        if not result or not result[0]:
            print(f"Column {variable_name_lower} does not exist in table {form_name_lower}")
            return None
            
        # Check if pregid column exists
        pregid_exists_query = text("""
            SELECT EXISTS (
                SELECT FROM information_schema.columns 
                WHERE table_name = :table_name 
                AND column_name = 'pregid'
            );
        """)
        
        result = db.session.execute(pregid_exists_query, {'table_name': form_name_lower}).fetchone()
        print(f"Pregid column exists result: {result}")
        if not result or not result[0]:
            print(f"Pregid column does not exist in table {form_name_lower}")
            return None
            
        # Build query based on whether form should have type_visit column
        form_requires_type_visit = form_name.lower() in FORMS_WITH_TYPE_VISIT
        form_no_type_visit = form_name.lower() in FORMS_WITHOUT_TYPE_VISIT
        
        print(f"Form {form_name} requires type_visit: {form_requires_type_visit}")
        print(f"Form {form_name} does not use type_visit: {form_no_type_visit}")
        
        if form_requires_type_visit:
            # Check if type_visit column exists in this table
            type_visit_exists_query = text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.columns 
                    WHERE table_name = :table_name 
                    AND column_name = 'type_visit'
                );
            """)
            
            result = db.session.execute(type_visit_exists_query, {'table_name': form_name_lower}).fetchone()
            print(f"Type_visit column exists result: {result}")
            if result and result[0]:
                # Form has type_visit column, include it in query
                query = text(f"""
                    SELECT {variable_name_lower} 
                    FROM {form_name_lower} 
                    WHERE pregid = :pregid 
                    AND type_visit = :visit_type
                    LIMIT 1;
                """)
                print(f"Executing query with type_visit: {query}")
                result = db.session.execute(query, {
                    'pregid': pregid,
                    'visit_type': visit_type
                }).fetchone()
                print(f"Query result with type_visit: {result}")
            else:
                # Form should have type_visit but doesn't, fallback to pregid only
                query = text(f"""
                    SELECT {variable_name_lower} 
                    FROM {form_name_lower} 
                    WHERE pregid = :pregid
                    LIMIT 1;
                """)
                print(f"Executing fallback query without type_visit: {query}")
                result = db.session.execute(query, {'pregid': pregid}).fetchone()
                print(f"Fallback query result: {result}")
        else:
            # Form doesn't require type_visit column, query by pregid only
            query = text(f"""
                SELECT {variable_name_lower} 
                FROM {form_name_lower} 
                WHERE pregid = :pregid
                LIMIT 1;
            """)
            print(f"Executing query without type_visit: {query}")
            result = db.session.execute(query, {'pregid': pregid}).fetchone()
            print(f"Query result without type_visit: {result}")
        
        if result:
            print(f"Found value: {result[0]}")
            return result[0]
        print("No result found")
        return None
        
    except Exception as e:
        print(f"Error fetching current value: {e}")
        import traceback
        traceback.print_exc()
        return None

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
                flash(f'File uploaded and processed successfully: {message}')
                return redirect(url_for('view_queries'))
            else:
                flash(f'Error processing file: {message}')
        else:
            flash('Invalid file type. Please upload an Excel file (.xlsx or .xls)')
    
    return render_template('upload.html')

@app.route('/queries')
def view_queries():
    try:
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
    except Exception as e:
        flash(f'Database error: {str(e)}')
        return redirect(url_for('upload_file'))

@app.route('/api/queries')
def api_queries():
    try:
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
    except Exception as e:
        return jsonify({'success': False, 'message': f'Database error: {str(e)}'}), 500

@app.route('/api/query/<int:query_id>', methods=['PUT'])
def update_query(query_id):
    try:
        query = Query.query.get_or_404(query_id)
        data = request.get_json()
        
        # Update only the fields that are provided (removed EditType)
        if 'Notes' in data:
            query.Notes = data['Notes']
        if 'status' in data:
            query.status = data['status']
        if 'RemoveEdit' in data:
            query.RemoveEdit = data['RemoveEdit']
        
        query.updated_at = datetime.utcnow()
        
        db.session.commit()
        return jsonify({'success': True, 'message': 'Query updated successfully'})
    except Exception as e:
        db.session.rollback()
        return jsonify({'success': False, 'message': str(e)}), 400

@app.route('/api/query/<int:query_id>/compare', methods=['GET'])
def compare_query_data(query_id):
    """Compare query value with current database value using your exact logic"""
    try:
        query = Query.query.get_or_404(query_id)
        
        print(f"Comparing query ID: {query_id}")
        print(f"Form: {query.Form}, VisitType: {query.VisitType}, PregID: {query.PregID}, Variable_Name: {query.Variable_Name}")
        
        # Get current value from the form table using your exact logic
        current_value = None
        comparison_result = None
        error_message = None
        query_used = None
        
        # Check if required fields exist
        if not query.Form:
            return jsonify({'success': False, 'message': 'Form field is missing'}), 400
        if not query.PregID:
            return jsonify({'success': False, 'message': 'PregID field is missing'}), 400
        if not query.Variable_Name:
            return jsonify({'success': False, 'message': 'Variable_Name field is missing'}), 400
            
        if query.Form and query.PregID and query.Variable_Name:
            current_value = get_current_value_from_form(
                query.Form, 
                query.VisitType if query.VisitType else '', 
                query.PregID, 
                query.Variable_Name
            )
            
            # Determine which query was actually used
            form_requires_type_visit = query.Form.lower() in FORMS_WITH_TYPE_VISIT
            form_no_type_visit = query.Form.lower() in FORMS_WITHOUT_TYPE_VISIT
            
            if form_requires_type_visit and query.VisitType:
                # Check if type_visit column exists
                try:
                    type_visit_exists_query = text("""
                        SELECT EXISTS (
                            SELECT FROM information_schema.columns 
                            WHERE table_name = :table_name 
                            AND column_name = 'type_visit'
                        );
                    """)
                    result = db.session.execute(type_visit_exists_query, {'table_name': query.Form.lower()}).fetchone()
                    if result and result[0]:
                        query_used = f"SELECT {query.Variable_Name} FROM {query.Form} WHERE pregid='{query.PregID}' AND type_visit='{query.VisitType}'"
                    else:
                        query_used = f"SELECT {query.Variable_Name} FROM {query.Form} WHERE pregid='{query.PregID}'"
                except Exception as e:
                    print(f"Error checking type_visit column: {e}")
                    query_used = f"SELECT {query.Variable_Name} FROM {query.Form} WHERE pregid='{query.PregID}'"
            else:
                query_used = f"SELECT {query.Variable_Name} FROM {query.Form} WHERE pregid='{query.PregID}'"
            
            print(f"Current value: {current_value}, Query value: {query.Variable_Value}")
            
            if current_value is not None:
                # Compare values
                query_value = query.Variable_Value
                if str(current_value) == str(query_value):
                    comparison_result = "MATCH"
                else:
                    comparison_result = "MISMATCH"
            else:
                comparison_result = "NOT_FOUND"
                error_message = "No data found for the specified criteria"
        else:
            comparison_result = "INCOMPLETE"
            error_message = "Missing required fields for comparison"
            query_used = "Incomplete query parameters"
        
        return jsonify({
            'success': True,
            'query_value': query.Variable_Value,
            'current_value': current_value,
            'comparison_result': comparison_result,
            'error_message': error_message,
            'form': query.Form,
            'visit_type': query.VisitType,
            'pregid': query.PregID,
            'variable_name': query.Variable_Name,
            'query_used': query_used
        })
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'message': str(e)}), 500

# BULK OPERATIONS ENDPOINTS

@app.route('/api/bulk/status', methods=['PUT'])
def bulk_update_status():
    try:
        data = request.get_json()
        query_ids = data.get('query_ids', [])
        new_status = data.get('status')
        
        if not query_ids or not new_status:
            return jsonify({'success': False, 'message': 'Missing query IDs or status'}), 400
        
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
    try:
        data = request.get_json()
        query_ids = data.get('query_ids', [])
        updates = data.get('updates', {})
        
        if not query_ids or not updates:
            return jsonify({'success': False, 'message': 'Missing query IDs or updates'}), 400
        
        # Remove EditType from bulk updates if present
        if 'EditType' in updates:
            del updates['EditType']
        
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
    try:
        data = request.get_json()
        query_ids = data.get('query_ids', [])
        
        if not query_ids:
            return jsonify({'success': False, 'message': 'Missing query IDs'}), 400
        
        deleted_count = Query.query.filter(Query.id.in_(query_ids)).delete(synchronize_session=False)
        db.session.commit()
        return jsonify({'success': True, 'message': f'Deleted {deleted_count} queries'})
    except Exception as e:
        db.session.rollback()
        return jsonify({'success': False, 'message': str(e)}), 400

@app.route('/api/bulk/export')
def bulk_export_queries():
    try:
        query_ids = request.args.getlist('query_ids')
        
        if not query_ids:
            return jsonify({'success': False, 'message': 'No queries selected'}), 400
        
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