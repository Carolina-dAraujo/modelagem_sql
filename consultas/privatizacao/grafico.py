import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# Carregar os dados
df = pd.read_csv('resultado.csv')  # Substitua pelo caminho correto

# Verificar colunas
print(df.columns)  # Para garantir que 'perc_privado' está presente

# Criar coluna de faixas de percentual
def classificar_faixa(p):
    if pd.isna(p):
        return 'sem dado'
    elif p <= 25:
        return '0-25%'
    elif p <= 50:
        return '26-50%'
    elif p <= 75:
        return '51-75%'
    else:
        return '76-100%'

df['faixa_privado'] = df['perc_privado'].apply(classificar_faixa)

# Plotar o gráfico de contagem
plt.figure(figsize=(8, 5))
ax = sns.countplot(
    data=df,
    x='faixa_privado',
    order=['0-25%', '26-50%', '51-75%', '76-100%'],
    palette='Blues'
)

plt.title('Número de Municípios por Faixa de Investimento Privado')
plt.xlabel('% de Investimento Privado (faixa)')
plt.ylabel('Quantidade de Municípios')

# Mostrar valores nas barras
for p in ax.patches:
    height = p.get_height()
    ax.annotate(f'{height}', (p.get_x() + p.get_width() / 2., height),
                ha='center', va='bottom')

plt.tight_layout()
plt.savefig('barra_desempenho_por_faixa.png')
