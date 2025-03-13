from flask import Flask, jsonify

app = Flask(__name__) #O app inicia somente se for diretamente (__name__)


@app.route('/pedidos', methods=['GET'])
def get_pedidos ():
    pedidos = [
        {
        'id': 1,
        'cliente': 'Empresa A',
        'status': 'Pago',
        'produtos': [
            {'id': 2, 'quantidade': 4},
            {'id': 3, 'quantidade': 3}
            ]
        
    },
    {
        'id': 2,
        'cliente': 'Empresa B',
        'status': 'Pendente',
        'produtos': [
            {'id': 1, 'quantidade': 2},
            {'id': 2, 'quantidade': 5}
            ] 
        
    }
]

    return jsonify(pedidos)

# Iniciando o servidor
if __name__ == '__main__':
    app.run(debug=True, port=5000) # debug=True para mostrar erros no navegador