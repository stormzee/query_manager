from flask_sqlalchemy import SQLAlchemy
from datetime import datetime

db = SQLAlchemy()

class Query(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    QueryID = db.Column(db.String(225), unique=True, nullable=False)
    GHAID = db.Column(db.String(7))
    ScrnID = db.Column(db.String(64))
    MomID = db.Column(db.String(64))
    PregID = db.Column(db.String(64))
    InfantID = db.Column(db.String(64))
    VisitType = db.Column(db.String(128))
    VisitDate = db.Column(db.Date)
    Form = db.Column(db.String(500))
    Variable_Name = db.Column(db.String(500))
    Variable_Value = db.Column(db.String(500))
    EditType = db.Column(db.String(500))
    UploadDate = db.Column(db.Date)
    FieldType = db.Column(db.String(500))
    DateEditReported = db.Column(db.Date)
    Form_Edit_Type = db.Column(db.String(500))
    VarFormEdit = db.Column(db.String(500))
    RemoveEdit = db.Column(db.String, nullable=True)
    Notes = db.Column(db.Text)
    status = db.Column(db.String(50), default='Pending')  # New field for status
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def to_dict(self):
        return {
            'id': self.id,
            'QueryID': self.QueryID,
            'GHAID': self.GHAID,
            'ScrnID': self.ScrnID,
            'MomID': self.MomID,
            'PregID': self.PregID,
            'InfantID': self.InfantID,
            'VisitType': self.VisitType,
            'VisitDate': self.VisitDate.strftime('%Y-%m-%d') if self.VisitDate else None,
            'Form': self.Form,
            'Variable_Name': self.Variable_Name,
            'Variable_Value': self.Variable_Value,
            'EditType': self.EditType,
            'UploadDate': self.UploadDate.strftime('%Y-%m-%d') if self.UploadDate else None,
            'FieldType': self.FieldType,
            'DateEditReported': self.DateEditReported.strftime('%Y-%m-%d') if self.DateEditReported else None,
            'Form_Edit_Type': self.Form_Edit_Type,
            'VarFormEdit': self.VarFormEdit,
            'RemoveEdit': self.RemoveEdit,
            'Notes': self.Notes,
            'status': self.status,
            'created_at': self.created_at.strftime('%Y-%m-%d %H:%M:%S') if self.created_at else None,
            'updated_at': self.updated_at.strftime('%Y-%m-%d %H:%M:%S') if self.updated_at else None,
        }
