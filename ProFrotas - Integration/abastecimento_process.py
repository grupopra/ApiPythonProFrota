#!/usr/bin/env python3
import requests
import pandas as pd
import json
from typing import Dict, List, Optional
from datetime import datetime
import time
import os

class AbastecimentoProcessor:
    def __init__(self, base_url: str, tenant_uuid: str, auth_token: str = None):
        self.base_url = base_url
        self.tenant_uuid = tenant_uuid
        # Token mockado - substitua pelo seu token
        self.auth_token = auth_token or "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyIjp7ImlkIjoxLCJlbWFpbCI6ImRlZmF1bHQudXNlckBncnVwb3ByYS50ZWNoIiwiaXNBcGkiOmZhbHNlfSwidGVuYW50Ijp7InV1aWQiOiJlMjNiNTIyMC1kYTdlLTQxNGQtYTc3My0yNDJjMGZjZTJjNWQifSwiaWF0IjoxNzU2NzM1MzI1LCJleHAiOjE3NTY3Nzg1MjV9.FW6Q8evr4D-Ttr5b98Zal__WiWET2MYtT14HKKW7rnw"
        self.email = "default.user@grupopra.tech"
        self.password = "GrupoPra@Tech!2025"
        self.max_retries = 3
        self.retry_delay = 2  
        
        self.static_data = {
            'allAbastecimentos': [],
            'results': [],
            'currentIndex': 0,
            'totalAbastecimentos': 0
        }
        
        # Inicializar headers
        self.update_headers()
    
    def update_headers(self):
        self.headers = {
            'accept': 'application/json',
            'Content-Type': 'application/json',
            'x-tenant-uuid': self.tenant_uuid,
            'x-tenant-user-auth': self.auth_token
        }
    
    def authenticate(self) -> bool:
        try:
            print("🔐 Realizando login automático...")
            
            auth_payload = {
                "email": self.email,
                "password": self.password
            }
            
            response = requests.post(
                f"{self.base_url}/auth/signIn",
                json=auth_payload
            )
            response.raise_for_status()
            
            auth_data = response.json()
            if auth_data and 'data' in auth_data and 'accessToken' in auth_data['data']:
                self.auth_token = auth_data['data']['accessToken']
                self.update_headers()
                print("✅ Login realizado com sucesso!")
                return True
            else:
                print("❌ Token de acesso não encontrado na resposta")
                return False
                
        except requests.exceptions.RequestException as e:
            print(f"❌ Erro ao realizar login: {e}")
            return False
    
    def make_request_with_retry(self, method: str, url: str, **kwargs) -> Optional[requests.Response]:
        """Faz uma requisição com retry automático e reautenticação em caso de 401"""
        for attempt in range(self.max_retries):
            try:
                if method.upper() == 'GET':
                    response = requests.get(url, headers=self.headers, **kwargs)
                elif method.upper() == 'POST':
                    response = requests.post(url, headers=self.headers, **kwargs)
                else:
                    print(f"❌ Método HTTP não suportado: {method}")
                    return None
                
                # Se sucesso, retornar a resposta
                if response.status_code == 200 or response.status_code == 201:
                    return response
                
                # Se erro 401, tentar reautenticar
                if response.status_code == 401:
                    print(f"⚠️ Erro 401 (Unauthorized) - Tentativa {attempt + 1}/{self.max_retries}")
                    if self.authenticate():
                        print("🔄 Reautenticado, tentando novamente...")
                        continue
                    else:
                        print("❌ Falha na reautenticação")
                        return None
                
                # Para outros erros, fazer retry com delay
                if attempt < self.max_retries - 1:
                    print(f"⚠️ Erro {response.status_code} - Tentativa {attempt + 1}/{self.max_retries}")
                    print(f"⏳ Aguardando {self.retry_delay} segundos antes de tentar novamente...")
                    time.sleep(self.retry_delay)
                    continue
                else:
                    print(f"❌ Erro {response.status_code} após {self.max_retries} tentativas")
                    return response
                    
            except requests.exceptions.RequestException as e:
                if attempt < self.max_retries - 1:
                    print(f"⚠️ Erro de conexão - Tentativa {attempt + 1}/{self.max_retries}: {e}")
                    print(f"⏳ Aguardando {self.retry_delay} segundos antes de tentar novamente...")
                    time.sleep(self.retry_delay)
                    continue
                else:
                    print(f"❌ Erro de conexão após {self.max_retries} tentativas: {e}")
                    return None
        
        return None
    
    def load_abastecimentos_from_sheet(self, sheet_data: List[Dict]) -> None:
        print("📋 Inicializando processamento...")
        
        self.static_data['allAbastecimentos'] = sheet_data
        self.static_data['totalAbastecimentos'] = len(sheet_data)
        
        print(f"📋 Iniciando processamento de {len(sheet_data)} registros de abastecimento")
        
        if sheet_data:
            print(f"📋 Exemplos de registros:")
            for i, abastecimento in enumerate(sheet_data[:3]):
                print(f"  {i+1}. Placa: {abastecimento.get('Placa', 'N/A')} | Data Transação: {abastecimento.get('Data Transação', 'N/A')}")
    
    def fetch_vehicle_by_plate(self, plate: str) -> Optional[Dict]:
        """Busca veículo por placa na API"""
        params = {
            'page': 1,
            'limit': 1,
            'license_plate': plate
        }
        
        response = self.make_request_with_retry(
            'GET',
            f"{self.base_url}/vehicle",
            params=params
        )
        
        if response and response.status_code in [200, 201]:
            vehicle_data = response.json()
            if vehicle_data and 'data' in vehicle_data and vehicle_data['data']:
                return vehicle_data['data'][0]
        
        return None
    
    def fetch_person_by_cpf(self, cpf: str) -> Optional[Dict]:
        """Busca pessoa por CPF na API"""
        # Limpar CPF removendo pontos e hífens
        clean_cpf = ''.join(filter(str.isdigit, str(cpf)))
        
        params = {
            'page': 1,
            'limit': 1,
            'cpf': clean_cpf
        }
        
        response = self.make_request_with_retry(
            'GET',
            f"{self.base_url}/person",
            params=params
        )
        
        if response and response.status_code in [200, 201]:
            person_data = response.json()
            if person_data and 'data' in person_data and person_data['data']:
                return person_data['data'][0]
        
        return None
    
    def fetch_supplier_by_cnpj(self, cnpj: str) -> Optional[Dict]:
        """Busca fornecedor por CNPJ na API"""
        clean_cnpj = ''.join(filter(str.isdigit, str(cnpj)))
        
        params = {
            'page': 1,
            'limit': 1,
            'cnpjs': [clean_cnpj] 
        }
        
        response = self.make_request_with_retry(
            'GET',
            f"{self.base_url}/supplier",
            params=params
        )
        
        if response and response.status_code in [200, 201]:
            supplier_data = response.json()
            if supplier_data and 'data' in supplier_data and supplier_data['data']:
                return supplier_data['data'][0]
        
        return None
    
    def fetch_company_by_id(self, company_id: str) -> Optional[Dict]:
        """Busca empresa por ID na API"""
        response = self.make_request_with_retry(
            'GET',
            f"{self.base_url}/company/{company_id}"
        )
        
        if response and response.status_code in [200, 201]:
            return response.json()
        
        return None
    
    def fetch_product_by_name(self, name: str) -> Optional[Dict]:
        """Busca produto por nome na API"""
        params = {
            'page': 1,
            'limit': 1,
            'name': name
        }
        
        response = self.make_request_with_retry(
            'GET',
            f"{self.base_url}/product",
            params=params
        )
        
        if response and response.status_code in [200, 201]:
            product_data = response.json()
            if product_data and 'data' in product_data and product_data['data']:
                return product_data['data'][0]
        
        return None
    
    def check_abastecimento_exists(self, abastecimento_id: str) -> bool:
        """Verifica se o abastecimento já existe na API pelo ID"""
        try:
            response = self.make_request_with_retry(
                'GET',
                f"{self.base_url}/fuel-supply",
                params={'code': abastecimento_id}
            )
            
            if response and response.status_code in [200, 201]:
                fuel_supply_data = response.json()
                if fuel_supply_data and 'data' in fuel_supply_data and fuel_supply_data['data']:
                    # Se encontrou registros com esse código, já existe
                    return len(fuel_supply_data['data']) > 0
            
            return False
            
        except Exception as e:
            print(f"⚠️ Erro ao verificar existência do abastecimento {abastecimento_id}: {e}")
            return False
    
    def create_abastecimento(self, payload: Dict) -> bool:
        """Cria abastecimento na API"""
        response = self.make_request_with_retry(
            'POST',
            f"{self.base_url}/fuel-supply",
            json=payload
        )
        
        if response and response.status_code in [200, 201]:
            return True
        
        return False
        response = self.make_request_with_retry(
            'POST',
            f"{self.base_url}/fuel-supply",
            json=payload
        )
        
        if response and response.status_code in [200, 201]:
            return True
        
        return False
    
    def process_abastecimento(self, abastecimento: Dict) -> Dict:
        """Processa um registro de abastecimento individual"""
        placa = abastecimento.get('Placa', '')
        cpf_motorista = abastecimento.get('CPF Motorista', '')
        nome_motorista = abastecimento.get('Nome Motorista', '')
        cnpj_posto = abastecimento.get('CNPJ Posto', '')
        razao_social_posto = abastecimento.get('Razão Social Posto', '')
        id_abastecimento = abastecimento.get('ID Abastecimento', '')
        status_autorizacao = abastecimento.get('Status Autorização', '')
        latitude_posto = abastecimento.get('Latitude Posto', '')
        longitude_posto = abastecimento.get('Longitude Posto', '')
        valor_total = abastecimento.get('Valor Total Abastecimento', 0)
        hodometro = abastecimento.get('Hodômetro', 0)
        data_transacao = abastecimento.get('Data Transação', '')
        hora_transacao = abastecimento.get('Hora', '')
        items_json = abastecimento.get('Items JSON', '')
        
        print(f"⛽ Processando abastecimento {self.static_data['currentIndex'] + 1}/{self.static_data['totalAbastecimentos']}: {placa}")
        
        try:
            if id_abastecimento:
                print(f"🔍 Verificando se abastecimento {id_abastecimento} já existe...")
                if self.check_abastecimento_exists(str(id_abastecimento)):
                    print(f"✅ Abastecimento {id_abastecimento} já existe na API, pulando...")
                    return {
                        **abastecimento,
                        'processed': True,
                        'message': 'Abastecimento já existe na API',
                        'status': 'JA_EXISTE'
                    }
                else:
                    print(f"✅ Abastecimento {id_abastecimento} não existe, prosseguindo com cadastro...")
            
            # 1. Buscar veículo por placa
            vehicle_data = self.fetch_vehicle_by_plate(placa)
            if not vehicle_data:
                return {
                    **abastecimento,
                    'processed': False,
                    'message': 'Veículo não encontrado na API',
                    'status': 'VEICULO_NAO_ENCONTRADO'
                }
            
            # 2. Buscar pessoa por CPF do motorista
            person_data = None
            if cpf_motorista:
                person_data = self.fetch_person_by_cpf(cpf_motorista)
                if not person_data:
                    return {
                        **abastecimento,
                        'processed': False,
                        'message': f'Motorista não encontrado na API por CPF: {cpf_motorista}',
                        'status': 'MOTORISTA_NAO_ENCONTRADO'
                    }
            
            # 3. Buscar fornecedor por CNPJ do posto
            supplier_data = None
            if cnpj_posto:
                supplier_data = self.fetch_supplier_by_cnpj(cnpj_posto)
                if not supplier_data:
                    return {
                        **abastecimento,
                        'processed': False,
                        'message': f'Fornecedor não encontrado na API por CNPJ: {cnpj_posto}',
                        'status': 'FORNECEDOR_NAO_ENCONTRADO'
                    }
            
            # 4. Processar items JSON
            items = []
            if items_json:
                try:
                    # Tentar fazer parse do JSON
                    if isinstance(items_json, str):
                        items_data = json.loads(items_json)
                    else:
                        items_data = items_json
                    
                    for item in items_data:
                        nome_produto = item.get('nome', '')
                        quantidade = item.get('quantidade', 0)
                        valor_total_item = item.get('valorTotal', 0)
                        
                        if nome_produto:
                            # Buscar produto por nome
                            product_data = self.fetch_product_by_name(nome_produto)
                            if product_data:
                                items.append({
                                    "productId": product_data['id'],
                                    "value": valor_total_item,
                                    "quantity": quantidade
                                })
                            else:
                                print(f"⚠️ Produto não encontrado: {nome_produto}")
                                # Continuar mesmo sem o produto
                                items.append({
                                    "productId": 0,  # ID padrão se não encontrar
                                    "value": valor_total_item,
                                    "quantity": quantidade
                                })
                except json.JSONDecodeError as e:
                    print(f"⚠️ Erro ao processar JSON dos items: {e}")
                    return {
                        **abastecimento,
                        'processed': False,
                        'message': f'Erro ao processar JSON dos items: {e}',
                        'status': 'ERRO_JSON_ITEMS'
                    }
            
            # 5. Converter status de autorização
            status_mapping = {
                'Aprovada': 'SUPPLY_APPROVED',
                'Recusada': 'SUPPLY_REJECTED'
            }
            status_final = status_mapping.get(status_autorizacao, 'SUPPLY_APPROVED')
            
            # 6. Formatar data e hora
            data_final = ""
            if data_transacao and hora_transacao:
                try:
                    # Converter para formato ISO com parsing explícito
                    if isinstance(data_transacao, str):
                        # Tentar diferentes formatos de data para evitar confusão
                        try:
                            # Primeiro tentar formato brasileiro DD/MM/YYYY
                            data_obj = pd.to_datetime(data_transacao, format='%d/%m/%Y', errors='coerce')
                            if pd.isna(data_obj):
                                # Se falhar, tentar formato ISO YYYY-MM-DD
                                data_obj = pd.to_datetime(data_transacao, format='%Y-%m-%d', errors='coerce')
                            if pd.isna(data_obj):
                                # Se ainda falhar, usar parsing automático
                                data_obj = pd.to_datetime(data_transacao)
                        except:
                            data_obj = pd.to_datetime(data_transacao)
                    else:
                        data_obj = data_transacao
                    
                    if isinstance(hora_transacao, str):
                        # Tentar diferentes formatos de hora
                        try:
                            hora_obj = pd.to_datetime(hora_transacao, format='%H:%M:%S', errors='coerce')
                            if pd.isna(hora_obj):
                                hora_obj = pd.to_datetime(hora_transacao, format='%H:%M', errors='coerce')
                            if pd.isna(hora_obj):
                                hora_obj = pd.to_datetime(hora_transacao)
                        except:
                            hora_obj = pd.to_datetime(hora_transacao)
                    else:
                        hora_obj = hora_transacao
                    
                    # Combinar data e hora
                    data_completa = data_obj.replace(
                        hour=hora_obj.hour,
                        minute=hora_obj.minute,
                        second=hora_obj.second
                    )
                    data_final = data_completa.isoformat() + "Z"
                except Exception as e:
                    print(f"⚠️ Erro ao formatar data: {e}")
                    data_final = datetime.now().isoformat() + "Z"
            else:
                data_final = datetime.now().isoformat() + "Z"
            
            # 7. Montar payload final
            payload = {
                "vehicleId": vehicle_data['id'],
                "personId": person_data['id'] if person_data else 0,
                "supplierId": supplier_data['id'] if supplier_data else 0,
                "companyId": 3,  # Mockado conforme solicitado
                "code": str(id_abastecimento) if id_abastecimento else "",
                "status": status_final,
                "lat": float(latitude_posto) if latitude_posto else 0.0,
                "lon": float(longitude_posto) if longitude_posto else 0.0,
                "gasStationBrand": "IPIRANGA",  # Mockado conforme solicitado
                "value": float(valor_total) if valor_total else 0.0,
                "odometer": int(hodometro) if hodometro else 0,
                "date": data_final,
                "items": items
            }
            
            print(f"📋 Payload montado para {placa}:")
            print(f"   Vehicle ID: {payload['vehicleId']}")
            print(f"   Person ID: {payload['personId']}")
            print(f"   Supplier ID: {payload['supplierId']}")
            print(f"   Items: {len(items)} produtos")
            
            # 8. Criar abastecimento
            success = self.create_abastecimento(payload)
            
            if success:
                return {
                    **abastecimento,
                    'processed': True,
                    'message': 'Abastecimento criado com sucesso',
                    'status': 'CRIADO',
                    'payload': payload
                }
            else:
                return {
                    **abastecimento,
                    'processed': False,
                    'message': 'Erro ao criar abastecimento',
                    'status': 'ERRO_CRIACAO'
                }
                
        except Exception as e:
            return {
                **abastecimento,
                'processed': False,
                'message': f'Erro no processamento: {str(e)}',
                'status': 'ERRO_PROCESSAMENTO'
            }
    
    def process_all_abastecimentos(self) -> None:
        """Processa todos os registros de abastecimento"""
        print("🔄 Iniciando processamento de todos os registros...")
        
        for i, abastecimento in enumerate(self.static_data['allAbastecimentos']):
            self.static_data['currentIndex'] = i
            
            # Processar abastecimento
            result = self.process_abastecimento(abastecimento)
            
            # Salvar resultado
            self.static_data['results'].append(result)
            
            # Mostrar progresso
            progress = (i + 1) / self.static_data['totalAbastecimentos'] * 100
            print(f"📊 Progresso: {i + 1}/{self.static_data['totalAbastecimentos']} ({progress:.1f}%)")
            
            # Pequena pausa para não sobrecarregar a API
            time.sleep(0.5)
        
        print("🏁 Processamento finalizado!")
    
    def generate_report(self) -> Dict:
        """Gera relatório final em Excel"""
        print("📈 Gerando relatório final...")
        
        results = self.static_data['results']
        
        # Criar DataFrame para Excel
        excel_data = []
        
        for r in results:
            # Manter todas as colunas originais da planilha
            excel_data.append({
                # Colunas originais da planilha (mantidas exatamente como estão)
                'Placa': r.get('Placa', ''),
                'CPF Motorista': r.get('CPF Motorista', ''),
                'Nome Motorista': r.get('Nome Motorista', ''),
                'CNPJ Posto': r.get('CNPJ Posto', ''),
                'Razão Social Posto': r.get('Razão Social Posto', ''),
                'ID Abastecimento': r.get('ID Abastecimento', ''),
                'Status Autorização': r.get('Status Autorização', ''),
                'Latitude Posto': r.get('Latitude Posto', ''),
                'Longitude Posto': r.get('Longitude Posto', ''),
                'Valor Total Abastecimento': r.get('Valor Total Abastecimento', ''),
                'Hodômetro': r.get('Hodômetro', ''),
                'Data Transação': r.get('Data Transação', ''),
                'Hora': r.get('Hora', ''),
                'Items JSON': r.get('Items JSON', ''),
                
                # Colunas de status do processamento (adicionadas no final)
                'STATUS_PROCESSAMENTO': r.get('status', ''),
                'PROCESSADO': 'SIM' if r.get('processed', False) else 'NÃO',
                'MENSAGEM': r.get('message', ''),
                'PAYLOAD': json.dumps(r.get('payload', {}), ensure_ascii=False) if r.get('payload') else ''
            })
        
        # Criar DataFrame
        df = pd.DataFrame(excel_data)
        
        # Salvar em Excel
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        excel_file = f'relatorio_abastecimento_{timestamp}.xlsx'
        
        with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
            # Aba com dados detalhados (planilha original + status)
            df.to_excel(writer, sheet_name='Dados Completos', index=False)
            
            # Aba apenas com dados originais (para reprocessamento)
            colunas_originais = [
                'Placa', 'CPF Motorista', 'Nome Motorista', 'CNPJ Posto', 'Razão Social Posto',
                'ID Abastecimento', 'Status Autorização', 'Latitude Posto', 'Longitude Posto',
                'Valor Total Abastecimento', 'Hodômetro', 'Data Transação', 'Hora', 'Items JSON'
            ]
            df_original = df[colunas_originais]
            df_original.to_excel(writer, sheet_name='Dados Originais', index=False)
            
            # Aba com resumo
            summary_data = {
                'Métrica': [
                    'Total Processados',
                    'Criados com Sucesso',
                    'Já Existem na API',
                    'Veículos Não Encontrados',
                    'Motoristas Não Encontrados',
                    'Fornecedores Não Encontrados',
                    'Erros de JSON nos Items',
                    'Erros de Processamento',
                    'Erros de Criação'
                ],
                'Quantidade': [
                    len(results),
                    len([r for r in results if r.get('status') == 'CRIADO']),
                    len([r for r in results if r.get('status') == 'JA_EXISTE']),
                    len([r for r in results if r.get('status') == 'VEICULO_NAO_ENCONTRADO']),
                    len([r for r in results if r.get('status') == 'MOTORISTA_NAO_ENCONTRADO']),
                    len([r for r in results if r.get('status') == 'FORNECEDOR_NAO_ENCONTRADO']),
                    len([r for r in results if r.get('status') == 'ERRO_JSON_ITEMS']),
                    len([r for r in results if r.get('status') == 'ERRO_PROCESSAMENTO']),
                    len([r for r in results if r.get('status') == 'ERRO_CRIACAO'])
                ]
            }
            
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Resumo', index=False)
        
        print(f"📊 Relatório Excel salvo: {excel_file}")
        print(f"📋 Aba 'Dados Originais' criada para reprocessamento")
        
        return {
            'relatorio': {
                'arquivo_excel': excel_file,
                'timestamp': datetime.now().isoformat()
            }
        }
    
    def run(self, sheet_data: List[Dict]) -> Dict:
        """Executa o fluxo completo"""
        print("🚀 Iniciando processamento de abastecimentos...")
        print("=" * 60)
        
        # 1. Carregar dados da planilha
        self.load_abastecimentos_from_sheet(sheet_data)
        
        # 2. Processar todos os registros
        self.process_all_abastecimentos()
        
        # 3. Gerar relatório final
        report = self.generate_report()
        
        print("=" * 60)
        print("✅ Processamento concluído!")
        
        return report


def main():
    """Função principal - Carrega dados da planilha de abastecimento"""
    
    BASE_URL = "https://prafrota-be-bff-tenant-api.grupopra.tech"
    TENANT_UUID = "e23b5220-da7e-414d-a773-242c0fce2c5d"

    planilha = "ProFrotas - Integration/resource - abastecimento/Abastecimento-Julho.xlsx"
    
    try:
        print(f"📊 Carregando dados da planilha: {planilha}")
        
        # Ler a planilha
        df = pd.read_excel(planilha)
        
        # Converter para lista de dicionários
        sheet_data = df.to_dict('records')
        
        print(f"📊 Total de registros encontrados: {len(sheet_data)}")
        
        if not sheet_data:
            print("❌ Nenhum registro encontrado na planilha!")
            return
        
        # Mostrar primeiros registros
        print("\n📋 Primeiros 3 registros:")
        for i, registro in enumerate(sheet_data[:3]):
            print(f"  {i+1}. Colunas disponíveis: {list(registro.keys())}")
        
        # Verificar colunas obrigatórias
        colunas_esperadas = [
            'Placa', 'CPF Motorista', 'Nome Motorista', 'CNPJ Posto', 'Razão Social Posto', 'ID Abastecimento',
            'Status Autorização', 'Latitude Posto', 'Longitude Posto',
            'Valor Total Abastecimento', 'Hodômetro', 'Data Transação',
            'Hora', 'Items JSON'
        ]
        
        colunas_faltantes = [col for col in colunas_esperadas if col not in sheet_data[0].keys()]
        if colunas_faltantes:
            print(f"⚠️ Colunas faltantes na planilha: {colunas_faltantes}")
            print("💡 O script pode não funcionar corretamente sem essas colunas")
        else:
            print("✅ Todas as colunas esperadas foram encontradas na planilha")
        
        # Criar instância e executar
        processor = AbastecimentoProcessor(BASE_URL, TENANT_UUID)
        report = processor.run(sheet_data)
        
        # O relatório Excel já foi gerado na função generate_report()
        excel_file = report['relatorio']['arquivo_excel']
        print(f"📄 Relatório Excel salvo em '{excel_file}'")
        
    except FileNotFoundError:
        print(f"❌ Arquivo não encontrado: {planilha}")
        print("💡 Verifique se a planilha está no mesmo diretório do script")
    except Exception as e:
        print(f"❌ Erro ao carregar planilha: {e}")


if __name__ == "__main__":
    main()
