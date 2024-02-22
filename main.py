import os
import platform
import oracledb
from playwright.sync_api import sync_playwright, Playwright, expect, FrameLocator, Page, BrowserContext
from datetime import date, timedelta, datetime
import pandas as pd
import re
import random
import logging

HOME_PAGE = 'http://mdshift.ddns.com.br/shift/integracao/mdias/tasy/s00.iu.Menu.cls'
wb_prescriptions = []
user_agents = [
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7A) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36',
  'Mozilla/5.0 (iPad; CPU OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148'
]

def getConnection() -> dict:
    return {
        'un': '*',
        'host': '*',
        'service_name': '*',
        'pw': '*'
    }

def getOraclePath() -> str:
    path = None  # default suitable for Linux
    if platform.system() == "Darwin" and platform.machine() == "x86_64":   # macOS
        path = os.environ.get("HOME")+("/Downloads/instantclient_19_8")
    elif platform.system() == "Windows":
        path = r"C:\oracle\instantclient_19_21"
    return path

def getDados(frameLocator: FrameLocator) -> pd.DataFrame:
    frame = frameLocator
    # Cria dataframes vazio para serem usados na inserção dos campos do pedido e dos exames
    df_dados_pedidos = pd.DataFrame()
    df_exames = pd.DataFrame( columns=['exame_cliente', 'exame_apoio', 'descricao','data','hora','Erro','pedido_cliente'] )    

    # busca todas as 'img' do locator //*[@id="tbody_jtRelatorioImportacao"]
    table = frame.locator('xpath=//*[@id="tbody_jtRelatorioImportacao"]')
    img = table.locator('img')
    if img.count() < 1:
        logger.error('Não há imagens a serem clicadas')
        return pd.DataFrame()
    

    # itera sob a quantidade de imagens (i) da tela/página
    for x in range( img.count() ): 

        # A partir da img, localiza a coluna da data/hora
        # Se a data/hora for menor que os agora - 15 minutos, a execução é cancelada
        data = img.nth(x).locator('..').locator('..').locator('td').nth(2).text_content()
        hora = img.nth(x).locator('..').locator('..').locator('td').nth(3).text_content()
        datahora = f'{data} {hora}'
        datahora = datetime.strptime(datahora, '%d/%m/%Y %H:%M:%S')
        if datahora < ( datetime.now() - timedelta(minutes=15) ):
            print('Ultimo pedido encontrado')
            return pd.DataFrame()

        # clica sobre a [x] posição da img
        try:
            img.nth(x).click()
        except:
            logger.error('Erro aconteceu ao tentar clicar na imagem')
            return
        
        # # # Coletar dados do pedido
        # Aguarda o carregamento e coleta os dados dos pedidos
        try:
            frame.locator('xpath=//*[@id="jsonTable_enclosingNav_56"]/div/input[5]').wait_for()
        except:
            logger.error('Erro aconteceu por timeout ao aguardar o carregamento da tela')
            return
        pedidoCliente = frame.locator('xpath=//*[@id="control_40"]').text_content()                 # Numero do pedido cliente
        pedidoApoio = frame.locator('xpath=//*[@id="control_36"]').text_content()                   # Numero do pedido do laboratatório
        nomePaciente = frame.locator('xpath=//*[@id="control_41"]').text_content()                  # Nome do paciente
        situacao = frame.locator('xpath=//*[@id="control_43"]').text_content()                      # Situação (importado com sucesso ou falha)
        
        logger.info(f'Coletando informações de {nomePaciente} pedido {pedidoCliente}')
        # Cria um dicionário com os dados do pedido, coletados acima
        pedido = {
            'pedido_cliente' : pedidoCliente,
            'pedido_apoio' : pedidoApoio,
            'nome_paciente' : nomePaciente,
            'situacao' : situacao
        }

        # Cria o data frame, a partir do dicionário e concate com o dataframe caso exista. Criando um novo DF parcial, que será retornado no final de cada página
        df = pd.DataFrame.from_dict( pedido, orient='index' ).T
        df_dados_pedidos = pd.concat( [df,df_dados_pedidos] )

        # # # Coletar dados dos exames de cada pedido
        # Cria um controle da página. Usado para saber quando deverá clicar na proxima página.
        shouldGoNextPage = True
        
        # Cria um indice iteravel para controle no numero da linha do exame.
        indexIterration = 0
        
        while shouldGoNextPage:
            # Localiza todas as linhas de exames do pedido aberto
            trs = frame.locator( 'xpath=//*[@id="tbody_jtRelatorioImportacaoExame"]' ).locator('tr')    

            for tr in range(trs.count() ):
                # Obtem todos os dados da linha. Utiliza a Regular Expression (re) para dividir cada '\t' em um item da lista. O indexIterration controla qual a linha atual (inicia no zero)
                tds = [ re.split( r'\t+', x ) for x in frame.locator(f'xpath=//*[@id="tr_jtRelatorioImportacaoExame_{indexIterration}"]').all_inner_texts() ]
                # Alguns exames dão erro de 'de-para', gerando um erro pois a quantidade das colunas é menor que 6. Neste caso, é inserido um '-' nas posições 1,2
                if len(tds[0]) == 4:
                    tds[0].insert(1,'-')
                    tds[0].insert(2,'-')

                # Converte lista em dataframe
                df_exames_partial = pd.DataFrame(tds, columns=['exame_cliente', 'exame_apoio', 'descricao','data','hora','Erro'])

                # Adiciona uma coluna com a numero do pedido cliente, será usado como chave primaria
                df_exames_partial['pedido_cliente'] = pedidoCliente                

                # Concatena o DF parcial (da pagina) com o DF geral (de todos os exames do pedido)
                df_exames= pd.concat( [df_exames, df_exames_partial], axis=0 )
                indexIterration += 1
                

            # # # Avançar pagina
            # # obtem locator da seta (>)                
            nextPage = frame.locator('#nav_jtRelatorioImportacaoExame').locator('.jsonTable__nav__button--arrow').nth(-2)            
            
            shouldGoNextPage = nextPage.is_enabled()
            # Se o locator nextPage estiver habilitada, avança a pagina
            # O Locator só ficarará desabilitado se a pagina atual for a ultima. Então ele vai coletar os dados da ultima pagina e vai sair do loop
            if shouldGoNextPage:
                nextPage.click()
        # Fecha tela pedido após coletar todos os exames
        frame.locator('xpath=//*[@id="scsPopupGroupClose32"]').click()
        logger.info('Coletado com sucesso')
    
    # Retorna lista de Dataframes de cada pedido
    return [[df_dados_pedidos],[df_exames]]

def saveOrders(dataFrame: pd.DataFrame):
    df = dataFrame
    strConn = getConnection()

    un = strConn['un']
    host = strConn['host']
    service_name = strConn['service_name']
    pw = strConn['pw']
    oracledb.init_oracle_client(lib_dir=getOraclePath())
    
    with oracledb.connect(user=un, password=pw, host=host, service_name=service_name) as connection:
        with connection.cursor() as cursor:            
            for rw in df.values:
                rw = list(rw)                
                query = f"""merge into LAB_INTEG_PEDIDOS_SHIFT lis
                                using (select :pedidoApoio pedido_apoio, :nomePaciente nome_paciente, :situacao situacao,:prescricao prescricao, :atendimento atendimento from dual ) s on ( lis.prescricao = s.prescricao and lis.atendimento = s.atendimento )
                            when matched then update set lis.pedido_apoio = s.pedido_apoio, lis.situacao = s.situacao
                            when not matched then insert values (s.pedido_apoio,s.nome_paciente, s.situacao, s.prescricao, s.atendimento)"""                      
                try:
                    cursor.execute(query, rw)
                    connection.commit()
                except:
                    logger.error('Um erro aconteceu ao tentar salvar o pedido:')
                    logger.error(rw)

    logger.info('Informações dos pedidos salvas no banco de dados')

def saveExams(dataFrame: pd.DataFrame):
    df = dataFrame
    strConn = getConnection()

    un = strConn['un']
    host = strConn['host']
    service_name = strConn['service_name']
    pw = strConn['pw']
    oracledb.init_oracle_client(lib_dir=getOraclePath())
    
    with oracledb.connect(user=un, password=pw, host=host, service_name=service_name) as connection:
        with connection.cursor() as cursor:
            
            for rw in df.values:
                rw = list(rw)                
                query = f"""merge into LAB_INTEG_EXAMES_SHIFT lis
                                using (select :exame_cliente exameCliente, :exame_apoio exameApoio, :descricao descricao, :data data, :hora hora, :Erro Erro, :prescricao prescricao from dual ) s 
                                on ( lis.prescricao = s.prescricao and lis.exame_cliente = s.exameCliente )
                            when matched then update set lis.erro = s.erro
                            when not matched then insert values (s.exameCliente, s.exameApoio, s.descricao, s.data, s.hora, s.Erro, s.prescricao)"""
                try:
                    cursor.execute(query, rw)
                    connection.commit()
                except:
                    logger.error('Um erro aconteceu ao tentar salvar os exames:')
                    logger.error(rw)


    logger.info('Informações dos exames salvas no banco de dados')

def getPage(playwright: Playwright) -> BrowserContext:
    logger.info('Iniciando Programa')
    navegador = playwright.chromium.launch(headless=False,slow_mo=1000 ,args=["--start-maximized"])
    context = navegador.new_context(
        # user_agent=user_agents[ random.randint(0, len(user_agents)-1 ) ], 
        no_viewport=True,
        record_video_dir="videos/",
        record_video_size={"width": 1920, "height": 870}
    )
    return context

def goToListPage(page: Page) -> FrameLocator:
     # Clicar no menu inicial
    page.locator('xpath=//*[@id="menu_3000"]').wait_for()
    page.locator('xpath=//*[@id="menu_3000"]').click()
    logger.info('Clicou no menu inicial')

    #clicar em relatorio
    page.locator('xpath=//*[@id="menu_3004"]').wait_for()
    page.locator('xpath=//*[@id="menu_3004"]').click()
    logger.info('Clicou no relatorio')

    #clicar em relatorio de importacao
    page.locator('xpath=//*[@id="menu_HIS_R3010"]').wait_for()
    page.locator('xpath=//*[@id="menu_HIS_R3010"]').click()   
    logger.info('Clicou no relatorio de importação')
    return page.frame_locator('#iframe_43')

def run(playwright: Playwright, shouldSendWhatsMessage = True):    
    context = getPage(playwright=playwright)
    page = context.new_page()
    page.goto(HOME_PAGE)
    
    expect(page).not_to_have_title('Service Unavailable',timeout=1000)
    logger.info('Progama iniciado')
    
    if shouldSendWhatsMessage:
        isLogedPage = False
        pageWB = context.new_page()
        pageWB.goto('https://web.whatsapp.com/')
        
        while not isLogedPage:
            print('Aguardando leitura do QRCODE do Whatsapp')
            isLogedPage = pageWB.locator('xpath=//*[@id="app"]/div/div[2]/div[3]/header/div[2]/div/span/div[5]/div/span').first.is_visible()
            pageWB.wait_for_timeout(5000)
        print('Logado no Whatsapp')
        logger.info('Logado no Whatsapp')
    # obter frame
    frame = goToListPage(page)
    while(True):
        if page.locator('xpath=//*[@id="menu_3000"]').is_visible():
            frame = goToListPage(page)
        pedidos = pd.DataFrame()
        exames = pd.DataFrame()
        dataAtual = date.today().strftime('%d/%m/%Y')
        # dataIni = (date.today() - timedelta(days=1)).strftime('%d/%m/%Y')
        
        print(f'Execução iniciada - {datetime.now()}')
        logger.info('Iniciado')
        try:
            frame.locator('xpath=//*[@id="control_15"]').fill(dataAtual)        
            frame.locator('xpath=//*[@id="control_16"]').fill(dataAtual)
            frame.locator('xpath=//*[@id="control_6"]').click()

            logger.info(f'Filtrou usando {dataAtual} - {dataAtual}')
        except:
            page.wait_for_timeout(5000)
            logger.error('Erro ao realizar filtragem')
            page.reload()
            logger.info('Execução reiniciada')
            continue
        
        # Aguarda tempo de load da tela
        try:
            frame.locator('xpath=//*[@id="jsonTable_enclosingNav_29"]/div').wait_for()
        except:
            print('Precisa relogar')
            logger.error( 'Necessário efetuar reload da tela' )
            page.reload()
            logger.info('Execução reiniciada')
            continue
            
            
        osQtde = frame.locator('xpath=//*[@id="control_31"]').text_content()
        if osQtde == 0:        
            return
        
        # Controle de paginação
        shouldGoNextPage = True
        while shouldGoNextPage:
            # Obter Dados
            try:
                [dfDetalhes, dfExames] = getDados(frameLocator=frame)
            except Exception as e:
                print(dfDetalhes[0])
                logger.error(f'Erro Aconteceu ao obter dados. \n{e}')
                break
            
            if len(dfDetalhes[0] ) ==0: 
                logger.error('Tamanho do DataFrame é igual a zero')
                break
            
            dfDetalhes = pd.DataFrame(dfDetalhes[0] )
            dfExames = pd.DataFrame(dfExames[0] )
            pedidos = pd.concat([dfDetalhes ,pedidos])
            exames  = pd.concat([dfExames ,exames])

            # # # Avançar pagina
            # # obtem locator da seta (>)
            nextPage = frame.locator('#jsonTable_enclosingNav_29').locator('.jsonTable__nav__button--arrow').nth(-2)
            # Se o locator nextPage estiver habilitada, avança a pagina
            # O Locator só ficarará desabilitado se a pagina atual for a ultima. Então ele vai coletar os dados da ultima pagina e vai sair do loop
            shouldGoNextPage = nextPage.is_enabled()
            if shouldGoNextPage:
                nextPage.click()
        if len(pedidos) ==0: 
            continue
        
        pedidos['prescricao'] = [str(x).split('*')[0] for x in pedidos['pedido_cliente']  ]
        pedidos['atendimento'] = [str(x).split('*')[1] for x in pedidos['pedido_cliente']  ]
        pedidos = pedidos.drop('pedido_cliente',axis='columns')

        exames['prescricao'] = [str(x).split('*')[0] for x in exames['pedido_cliente']  ]
        exames_erro = exames[ 
            exames['Erro'].str.startswith('ERRO #5001: O CPF') |
            exames['Erro'].str.startswith('ERRO #5001: De/Para')
            ] [['Erro','pedido_cliente']].drop_duplicates()
        
        exames = exames.drop('pedido_cliente',axis='columns')
        exames.to_excel('Exames.xlsx',index=False)
        pedidos.to_excel('Pedidos.xlsx',index=False)
        
        # print('=========================')
        try:
            # saveOrders(pedidos)  
            # saveExams (exames)
            
            frame.locator('xpath=//*[@id="jsonTable_enclosingNav_29"]/div/input[1]').click()
            if shouldSendWhatsMessage:
                for index,row in exames_erro.iterrows():
                    if row['pedido_cliente'] not in wb_prescriptions:
                        print('Enviar erro por whatsapp')
                        logger.info('Enviar erro por whatsapp')
                        wb_prescriptions.append(row['pedido_cliente'])
                        text = f'Erro {row["Erro"]} aconteceu no pedido *{row["pedido_cliente"]}*'
                        if shouldSendWhatsMessage:
                            pageWB.locator('xpath=//*[@id="side"]/div[1]/div/div[2]/div[2]/div/div[1]/p').clear()
                            pageWB.locator('xpath=//*[@id="side"]/div[1]/div/div[2]/div[2]/div/div[1]/p').fill('Vinícius Labo')
                            pageWB.keyboard.press('Enter')
                            pageWB.locator('xpath=//*[@id="main"]/footer/div[1]/div/span[2]/div/div[2]/div[1]/div/div[1]/p').fill( text )
                            pageWB.locator('xpath=//*[@id="main"]/footer/div[1]/div/span[2]/div/div[2]/div[2]/button/span').click()
                            print('Mensagem enviada')
                            logger.info('Mensagem enviada')

            print(f'Execução finalizada - {datetime.now()}')
        except Exception as e:
            logger.error(f'Erro na linha 343\nErro{e}')
        page.wait_for_timeout(5000)

with sync_playwright() as playwright:
    # try:
        path = r'c:\repos\shift'
        if not os.path.exists(path + r'\Logs'):
            os.makedirs(path + r'\Logs')
        dataAtual = date.today().strftime('%d%m%Y%H%M%S')
        logging.basicConfig(
            filename= path + rf'\Logs\shift-{dataAtual}.log',
            # filename= path + r'\Logs\shift',
            level=logging.DEBUG,
            format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        logger = logging.getLogger('Shift')
        run(playwright, shouldSendWhatsMessage=False)
    # except Exception as err:
    #     logger.error( f'Ocorreu um erro: {err}' ) 