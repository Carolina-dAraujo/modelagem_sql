import pandas as pd
import mysql.connector
from mysql.connector import errorcode

# Configurações do banco
config = {
    'user': 'root',
    'password': 'minhasenha',
    'host': 'localhost',
    'database': 'mydb',
    'raise_on_warnings': True
}

# Conexão com o banco
def connect():
    try:
        conn = mysql.connector.connect(**config)
        print("Conectado ao banco de dados.")
        return conn
    except mysql.connector.Error as err:
        print(f"Erro de conexão: {err}")
        return None

# Função para desabilitar/habilitar checagem de chaves estrangeiras
def set_foreign_key_checks(conn, enable: bool):
    cursor = conn.cursor()
    valor = 1 if enable else 0
    cursor.execute(f"SET FOREIGN_KEY_CHECKS = {valor};")
    cursor.close()
    if enable:
        print("✔ Checagem de chaves estrangeiras ativada.")
    else:
        print("✔ Checagem de chaves estrangeiras desativada.")

# Função genérica para inserir um DataFrame em uma tabela, tratando NaNs como -1
def insert_dataframe(conn, df, table_name):
    cursor = conn.cursor()
    cols = ", ".join(df.columns)
    placeholders = ", ".join(["%s"] * len(df.columns))
    sql = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"

    for row in df.itertuples(index=False, name=None):
        row_clean = []
        for x in row:
            if pd.isna(x):
                row_clean.append(-1)
            else:
                # Garante tipo nativo do Python (int, float, str, bool)
                if isinstance(x, (int, float, str, bool)):
                    row_clean.append(x)
                else:
                    # Tenta converter, por exemplo numpy.int64 -> int
                    try:
                        row_clean.append(x.item())
                    except AttributeError:
                        row_clean.append(x)
        try:
            cursor.execute(sql, tuple(row_clean))
        except mysql.connector.Error as err:
            print(f"Erro ao inserir em {table_name}: {err}")

    conn.commit()
    cursor.close()
    print(f"✔ Tabela {table_name} populada com sucesso.")

# Define os tipos corretos de colunas para cada DataFrame
def load_csvs():
    tabelas = {}

    # 1. localidade
    tabelas['localidade'] = pd.read_csv('../tabelas_normalizadas/localidade.csv', dtype={
        'id_municipio': 'Int64',
        'populacao_urbana': float,
        'populacao_urbana_residente_agua': float,
        'populacao_urbana_atendida_agua': float,
        'populacao_urbana_residente_esgoto': float,
        'populacao_urbana_atendida_esgoto': float,
        'sigla_uf': str,
        'nome_uf': str,
        'nome_municipio': str,
        'status_municipio': str,
        'regiao': str
    })

    # 2. receitas
    tabelas['receitas'] = pd.read_csv('../tabelas_normalizadas/receitas.csv', dtype={
        'ano': 'Int64',
        'id_municipio': 'Int64',
        'receita_operacional_direta': float,
        'receita_operacional_direta_agua': float,
        'receita_operacional_direta_esgoto': float,
        'receita_operacional_indireta': float,
        'receita_operacional_direta_agua_exportada': float,
        'receita_operacional': float,
        'receita_operacional_direta_esgoto_importado': float
    })

    # 3. despesas
    tabelas['despesas'] = pd.read_csv('../tabelas_normalizadas/despesas.csv', dtype={
        'ano': 'Int64',
        'id_municipio': 'Int64',
        'despesa_pessoal': float,
        'despesa_produto_quimico': float,
        'despesa_energia': float,
        'despesa_servico_terceiro': float,
        'despesa_exploracao': float,
        'despesas_juros_divida': float,
        'despesa_total_servico': float,
        'despesa_ativo': float,
        'despesa_agua_importada': float,
        'despesa_fiscal': float,
        'despesa_fiscal_nao_computada': float,
        'despesa_exploracao_outro': float,
        'despesa_servico_outro': float,
        'despesa_amortizacao_divida': float,
        'despesas_juros_divida_excecao': float,
        'despesa_divida_variacao': float,
        'despesa_divida_total': float,
        'despesa_esgoto_exportado': float,
        'despesa_capitalizavel_municipio': float,
        'despesa_capitalizavel_estado': float,
        'despesa_capitalizavel_prestador': float
    })

    # 4. investimentos
    tabelas['investimentos'] = pd.read_csv('../tabelas_normalizadas/investimentos.csv', dtype={
        'ano': 'Int64',
        'id_municipio': 'Int64',
        'investimento_agua_prestador': float,
        'investimento_esgoto_prestador': float,
        'investimento_outro_prestador': float,
        'investimento_recurso_proprio_prestador': float,
        'investimento_recurso_oneroso_prestador': float,
        'investimento_recurso_nao_oneroso_prestador': float,
        'investimento_total_prestador': float,
        'investimento_agua_municipio': float,
        'investimento_esgoto_municipio': float,
        'investimento_outro_municipio': float,
        'investimento_recurso_proprio_municipio': float,
        'investimento_recurso_oneroso_municipio': float,
        'investimento_recurso_nao_oneroso_municipio': float,
        'investimento_total_municipio': float,
        'investimento_agua_estado': float,
        'investimento_esgoto_estado': float,
        'investimento_outro_estado': float,
        'investimento_recurso_proprio_estado': float,
        'investimento_recurso_oneroso_estado': float,
        'investimento_recurso_nao_oneroso_estado': float,
        'investimento_total_estado': float
    })

    # 5. indices
    tabelas['indices'] = pd.read_csv('../tabelas_normalizadas/indices.csv', dtype={
        'ano': 'Int64',
        'id_municipio': 'Int64',
        'indice_agua_ligacao': float,
        'indice_hidrometracao': float,
        'indice_macromedicao': float,
        'indice_perda_faturamento': float,
        'indice_coleta_esgoto': float,
        'indice_tratamento_esgoto': float,
        'indice_consumo_agua': float,
        'indice_fluoretacao_agua': float
    })

    # 6. infraestrutura
    tabelas['infraestrutura'] = pd.read_csv('../tabelas_normalizadas/infraestrutura.csv', dtype={
        'id_municipio': 'Int64',
        'extensao_rede_agua': float,
        'extensao_rede_esgoto': float,
        'quantidade_sede_municipal_agua': 'Int64',
        'quantidade_sede_municipal_esgoto': 'Int64',
        'quantidade_localidade_agua': 'Int64',
        'quantidade_localidade_esgoto': 'Int64',
        'quantidade_ligacao_ativa_agua': 'Int64',
        'quantidade_ligacao_ativa_esgoto': 'Int64',
        'volume_agua_produzido': float,
        'volume_esgoto_tratado': float
    })

    return tabelas

# Execução principal
def main():
    conn = connect()
    if not conn:
        return

    # Desabilita checagem de chave estrangeira para evitar erros de dependência
    set_foreign_key_checks(conn, enable=False)

    tabelas = load_csvs()

    for nome, df in tabelas.items():
        print(f"🔄 Populando tabela {nome} com {len(df)} registros...")
        insert_dataframe(conn, df, nome)

    # Reabilita checagem de chave estrangeira após inserir os dados
    set_foreign_key_checks(conn, enable=True)

    conn.close()
    print("✅ Finalizado com sucesso.")

if __name__ == "__main__":
    main()
