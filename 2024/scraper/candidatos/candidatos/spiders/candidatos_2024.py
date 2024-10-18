import json
from scrapy import Spider, Request


class Candidatos2024Spider(Spider):
    name = "2024"

    def start_requests(self):
        # Todos os municipios de SC
        url = "https://divulgacandcontas.tse.jus.br/divulga/rest/v1/eleicao/buscar/SC/2045202024/municipios"
        yield Request(url, self.parse_municipios)
        
    def parse_municipios(self, response):
        data = json.loads(response.text)
        municipios = data["municipios"]
        
        # Para cada municipio, buscar os candidatos
        for municipio in municipios:
            
            # 11 - Prefeitos, 12 - Vice-Prefeitos, 13 - Vereadores
            url_prefeitos = f"https://divulgacandcontas.tse.jus.br/divulga/rest/v1/candidatura/listar/2024/{municipio['codigo']}/2045202024/11/candidatos"
            url_vice_prefeitos = f"https://divulgacandcontas.tse.jus.br/divulga/rest/v1/candidatura/listar/2024/{municipio['codigo']}/2045202024/12/candidatos"
            url_vereadores = f"https://divulgacandcontas.tse.jus.br/divulga/rest/v1/candidatura/listar/2024/{municipio['codigo']}/2045202024/13/candidatos"
            
            # Busca os 3 tipos de candidatos para cada municipio
            yield Request(url_prefeitos, self.parse_candidatos)
            yield Request(url_vice_prefeitos, self.parse_candidatos)
            yield Request(url_vereadores, self.parse_candidatos)
            
    def parse_candidatos(self, response):
        data = json.loads(response.text)
        cargo = data["cargo"]['codigo']
        codigo_cidade = data['unidadeEleitoral']['codigo']
        candidatos = data["candidatos"]
        
        # Pegando as informações de cada candidato
        for candidato in candidatos:
            yield Request(f"https://divulgacandcontas.tse.jus.br/divulga/rest/v1/candidatura/buscar/2024/{codigo_cidade}/2045202024/candidato/{candidato['id']}", 
                self.parse_candidato,
                meta={"cargo": cargo, "id_candidato": candidato["id"]}
            )

    def parse_candidato(self, response):
        data = json.loads(response.text)
        cargo = response.meta["cargo"]
        id_candidato = response.meta["id_candidato"]
        
        dados_candidato = {
            "nome": data["nomeCompleto"],
            "numUrna": data["numero"],
            "sexo": data["descricaoSexo"],
            "dataNasc": data["dataDeNascimento"],
            "tituloEleitor": data["tituloEleitor"],
            "cidadeNasc": data["nomeMunicipioNascimento"],
            "estadoNasc": data["sgUfNascimento"],
            "cidadeCandidatura": data["localCandidatura"],
            "estadoCandidatura": data["ufSuperiorCandidatura"],
            "fotoUrl": data["fotoUrl"],
            "situacaoAtual": data["descricaoTotalizacao"],
            "bens": data["bens"],
            "vice": data["vices"][0].get("nm_CANDIDATO") if data["vices"] else None,
            "partido": data["partido"],
            "cargo": data["cargo"]["nome"]
        }
        
        yield Request(f'https://divulgacandcontas.tse.jus.br/divulga/rest/v1/prestador/consulta/2045202024/2024/{data["ufCandidatura"]}/{cargo}/{data["partido"]["numero"]}/{data["numero"]}/{id_candidato}',
            self.parse_financeiro,
            meta={"dados_candidato": dados_candidato}
        )
        
    def parse_financeiro(self, response):
        data = json.loads(response.text)
        dados_candidato = response.meta["dados_candidato"]
         
        dados_financeiros = {
            "totalLiquidoCampanha": data["dadosConsolidados"]["totalRecebido"],
            "despesas": {
                "limiteDeGastos": data["despesas"]["valorLimiteDeGastos"],
                "totalDespesasContratadas": data["despesas"]["totalDespesasContratadas"],
                "totalDespesasPagas": data["despesas"]["totalDespesasPagas"],
                "doacoesParaOutrosCandidatosOuPartidos": data["despesas"]["doacoesOutrosCandidatosPartigos"],
            },
            "concentracaoDespesas": data["concentracaoDespesas"],
            "doadores": data["rankingDoadores"],
            "fornecedores": data["rankingFornecedores"]
        }
        
        yield {**dados_candidato, **dados_financeiros}