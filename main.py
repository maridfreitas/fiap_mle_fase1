from datetime import datetime
from functools import wraps
from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from sqlalchemy import create_engine, Column, Integer, Float, String, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker
import scraper
import jwt
import os

users = {
    "teste1": "senha1",
    "teste2": "senha2"
}

security = HTTPBasic()

DB_URL = os.environ.get("DATABASE_URL")
engine = create_engine(DB_URL, echo=False)
Base = declarative_base()
SessionLocal = sessionmaker(bind=engine)


class Producao(Base):
    __tablename__ = 'producao'
    id = Column(Integer, primary_key=True, autoincrement=True)
    produto = Column(String(500), nullable=False)
    quantidade = Column(Integer, nullable=False)
    tipo = Column(String(255), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

class Processamento(Base):
    __tablename__ = 'processamento'
    id = Column(Integer, primary_key=True, autoincrement=True)
    cultivar = Column(String(500), nullable=False)
    quantidade = Column(Integer, nullable=False)
    tipo = Column(String(255), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

class Comercializacao(Base):
    __tablename__ = 'comercializacao'
    id = Column(Integer, primary_key=True, autoincrement=True)
    produto = Column(String(500), nullable=False)
    quantidade = Column(Integer, nullable=False)
    tipo = Column(String(255), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

class Importacao(Base):
    __tablename__ = 'importacao'
    id = Column(Integer, primary_key=True, autoincrement=True)
    pais = Column(String(500), nullable=False)
    quantidade = Column(Integer, nullable=False)
    valor = Column(Float, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

class Exportacao(Base):
    __tablename__ = 'exportacao'
    id = Column(Integer, primary_key=True, autoincrement=True)
    pais = Column(String(500), nullable=False)
    quantidade = Column(Integer, nullable=False)
    valor = Column(Float, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow) 

Base.metadata.create_all(engine)

app = FastAPI(
    title='API Otimização Vinicula',
    version='1.0.0',
    description='API para prever quantidade ideal de tipo de vinho que deve ser produzido.'
)

def verify_password(credentials: HTTPBasicCredentials = Depends(security)):
    username = credentials.username
    password = credentials.password
    if username in users and users[username] == password:
        return username
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail='Credenciais inválidas',
        headers={"WWW-Authenticate": "Basic"}
    )


def incluir_banco(df, tipo):
    for index, row in df.iterrows():
        db = SessionLocal()
        if tipo.upper() == 'PRODUCAO':
                new_row = Producao(
                    produto = row.produto,
                    quantidade = int(row.quantidade.replace('.','').replace('-','0')),
                    tipo = row.tipo
                )
        elif tipo.upper() == 'PROCESSAMENTO':
                new_row = Processamento(
                    cultivar = row.cultivar,
                    quantidade = int(row.quantidade.replace('.','').replace('-','0')),
                    tipo = row.tipo
                )
        elif tipo.upper() == 'COMERCIALIZACAO':
                new_row = Comercializacao(
                    produto = row.produto,
                    quantidade = int(row.quantidade.replace('.','').replace('-','0')),
                    tipo = row.tipo
                )
        elif tipo.upper() == 'IMPORTACAO':
                new_row = Importacao(
                    pais = row.pais,
                    quantidade = int(row.quantidade.replace('.','').replace('-','0')),
                    valor = float(row.valor.replace('.','').replace('-','0'))
                )
        elif tipo.upper() == 'EXPORTACAO':
            new_row = Exportacao(
                pais = row.pais,
                quantidade = int(row.quantidade.replace('.','').replace('-','0')),
                valor = float(row.valor.replace('.','').replace('-','0'))
            )
        db.add(new_row)
        db.commit()
        db.close()

incluir_banco(scraper.scrape_producao(), 'Producao')
incluir_banco(scraper.scrape_processamento(), 'Processamento')
incluir_banco(scraper.scrape_comercializacao(), 'Comercializacao')
incluir_banco(scraper.scrape_importacao(), 'Importacao')
incluir_banco(scraper.scrape_exportacao(), 'Exportacao')

@app.get('/')
async def home():
    return 'Olá, somos a API Otimização Vinicula!'


@app.get('/producao', summary='Pegar dados de Produção')
async def get_items_producao():
    db = SessionLocal()
    producao = db.query(Producao).all()
    db.close()
    return producao

@app.get('/processamento', summary='Pegar dados de Processamento')
async def get_items_processamento():
    db = SessionLocal()
    processamento = db.query(Processamento).all()
    db.close()
    return processamento

@app.get('/comercializacao', summary='Pegar dados de Comercialização')
async def get_items_comercializacao():
    db = SessionLocal()
    comercializacao = db.query(Comercializacao).all()
    db.close()
    return comercializacao

@app.get('/importacao', summary='Pegar dados de Importação')
async def get_items_importacao():
    db = SessionLocal()
    importacao = db.query(Importacao).all()
    db.close()
    return importacao

@app.get('/exportacao', summary='Pegar dados de Exportação')
async def get_items_exportacao():
    db = SessionLocal()
    exportacao = db.query(Exportacao).all()
    db.close()
    return exportacao

@app.post('/otimizacao', summary='Otimização da quantidade de vinhos a ser produzida')
async def otimizacao(username: str = Depends(verify_password)):
    return 'Em construção'