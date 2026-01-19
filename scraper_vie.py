#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Scraper VIE - SANS URL
Détection doublons sur titre + entreprise + lieu
"""

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from playwright.sync_api import sync_playwright
from datetime import datetime
import time
import sqlite3
import os

# ============================================
# CONFIGURATION
# ============================================

DB_FILE = 'offres_vie.db'

CRITERES = {
    'keywords': [
        # DATA
        'data', 'données', 'donnée', 'database', 'bd', 'base de données',
        'big data', 'données massives', 'data mining', 'exploration de données',
        
        # ENGINEER
        'engineer', 'ingénieur', 'ingenieur', 'ing', 'engineering', 'ingénierie',
        'software engineer', 'ingénieur logiciel', 'systems engineer',
        
        # DEVELOPER / DEV
        'developer', 'développeur', 'developpeur', 'dev', 'programmeur', 'programmer',
        'coder', 'codeur', 'software developer', 'application developer',
        
        # DATA ROLES
        'data engineer', 'ingénieur données', 'ingénieur data', 'data scientist',
        'data analyst', 'analyste données', 'analyste data', 'data architect',
        'architecte données', 'data platform engineer', 'etl developer',
        'etl engineer', 'data integration', 'intégration données',
        
        # ANALYST
        'analyst', 'analyste', 'analytics', 'analytique', 'business analyst',
        'analyste métier', 'analyste business', 'functional analyst',
        'analyste fonctionnel', 'systems analyst', 'analyste système',
        'financial analyst', 'analyste financier', 'reporting analyst',
        'insights analyst', 'operations analyst', 'process analyst',
        
        # SCIENTIST
        'scientist', 'scientifique', 'data scientist', 'research scientist',
        'chercheur', 'applied scientist', 'science des données',
        
        # BI & REPORTING
        'business intelligence', 'bi', 'intelligence économique',
        'bi developer', 'bi analyst', 'reporting', 'visualisation',
        'data visualization', 'dataviz', 'data viz', 'tableau', 'power bi',
        
        # MACHINE LEARNING / AI
        'machine learning', 'ml', 'apprentissage automatique',
        'ml engineer', 'mlops', 'ai', 'ia', 'artificial intelligence',
        'intelligence artificielle', 'deep learning', 'apprentissage profond',
        'nlp', 'natural language processing', 'traitement du langage',
        'computer vision', 'vision par ordinateur', 'neural network',
        
        # ARCHITECTURE
        'architect', 'architecte', 'solution architect', 'architecte solutions',
        'enterprise architect', 'cloud architect', 'architecte cloud',
        'technical architect', 'architecte technique',
        
        # DEVOPS / SRE
        'devops', 'dev ops', 'sre', 'site reliability', 'infrastructure',
        'platform engineer', 'ingénieur plateforme', 'automation',
        'automatisation', 'ci/cd', 'continuous integration',
        
        # BACKEND / FULLSTACK
        'backend', 'back-end', 'backend developer', 'développeur backend',
        'fullstack', 'full stack', 'full-stack', 'développeur fullstack',
        'fullstack developer', 'full stack developer',
        
        # LANGAGES PROGRAMMATION
        'python', 'java', 'javascript', 'c++', 'cpp', 'c#', 'csharp',
        'sql', 'nosql', 'cobol', 'scala', 'r', 'go', 'golang',
        'typescript', 'php', 'ruby', '.net', 'dotnet',
        
        # CLOUD / INFRA
        'cloud', 'aws', 'azure', 'gcp', 'google cloud', 'cloud computing',
        'kubernetes', 'docker', 'container', 'microservices', 'api',
        
        # DATA STORAGE
        'data warehouse', 'entrepôt de données', 'data lake', 'lac de données',
        'data platform', 'plateforme données', 'etl', 'elt', 'data hub',
        'data mart', 'datalake', 'datawarehouse',
        
        # TESTING / QA
        'testing', 'test', 'qa', 'quality assurance', 'qualité',
        'tester', 'testeur', 'test engineer', 'ingénieur test',
        'automation testing', 'test automation', 'qa engineer',
        
        # PRODUCT / MANAGEMENT
        'product', 'produit', 'product manager', 'chef de produit',
        'product owner', 'po', 'project manager', 'chef de projet',
        'scrum master', 'agile', 'technical lead', 'tech lead',
        
        # TECH GENERAL
        'tech', 'technology', 'technologie', 'it', 'informatique',
        'software', 'logiciel', 'application', 'system', 'système',
        'digital', 'numérique', 'innovation', 'transformation digitale',
        
        # BUSINESS / MANAGEMENT
        'business', 'métier', 'consultant', 'consulting', 'conseil',
        'strategy', 'stratégie', 'technical', 'technique', 'support',
        'specialist', 'spécialiste', 'expert', 'coordinator', 'coordinateur',
        
        # QUANT / FINANCE
        'quant', 'quantitative', 'quantitative analyst', 'quant developer',
        'quantitative researcher', 'financial engineer', 'ingénieur financier',
        'risk', 'risque', 'risk analyst', 'trading', 'trader',
        
        # DATA GOVERNANCE / MANAGEMENT
        'data governance', 'gouvernance données', 'data quality',
        'qualité données', 'data management', 'gestion données',
        'master data', 'mdm', 'metadata', 'métadonnées',
        
        # WEB / FRONTEND 
        'web', 'frontend', 'front-end', 'ux', 'ui', 'interface',
        'react', 'angular', 'vue', 'javascript developer',

        
        # ============================================
        # FINANCE / BANCAIRE

        # ASSET MANAGEMENT
        'asset', 'asset management', 'gestion d\'actifs', 'gestion actifs',
        'portfolio', 'portefeuille', 'fund', 'funds', 'fonds',
        'etf', 'mutual fund', 'opcvm', 'sicav', 'fcp',
        'hedge fund', 'private equity', 'investment', 'investissement',
        'wealth management', 'gestion de patrimoine',
        
        # TRADING / MARKETS
        'trading', 'trader', 'market', 'marché', 'marchés',
        'equity', 'equities', 'actions', 'fixed income', 'obligataire',
        'derivatives', 'dérivés', 'options', 'futures', 'swaps',
        'forex', 'fx', 'commodities', 'matières premières',
        'crypto', 'cryptocurrency', 'blockchain', 'bitcoin',
        
        # FRONT OFFICE
        'front office', 'sales', 'trading desk', 'desk',
        'execution', 'exécution', 'order management', 'oms',
        'trade', 'transaction', 'deal', 'booking',
        
        # MIDDLE OFFICE
        'middle office', 'control', 'contrôle', 'reconciliation',
        'rapprochement', 'settlement', 'règlement-livraison',
        'collateral', 'collatéral', 'margin', 'marge',
        
        # RISK MANAGEMENT
        'risk', 'risque', 'var', 'value at risk', 'credit risk',
        'risque de crédit', 'market risk', 'risque de marché',
        'operational risk', 'risque opérationnel', 'stress test',
        'compliance', 'conformité', 'regulatory', 'réglementaire',
        
        # QUANT / ALGO TRADING
        'quant', 'quantitative', 'quantitative analyst',
        'algo', 'algorithmic', 'algorithmique', 'algo trading',
        'high frequency', 'hft', 'market making', 'arbitrage',
        'pricing', 'pricer', 'valorisation', 'valuation',
        
        # FINTECH / TRADING SYSTEMS
        'fintech', 'trading system', 'système de trading',
        'trading platform', 'plateforme de trading',
        'market data', 'données de marché', 'real-time', 'temps réel',
        'low latency', 'faible latence', 'performance',
        
        # IT BANCAIRE / FINANCE
        'banking', 'banque', 'bank', 'financial', 'financier',
        'finance it', 'it finance', 'financial services',
        'services financiers', 'capital markets', 'marchés de capitaux',
        'investment banking', 'banque d\'investissement',
        
        # SYSTÈMES & APPLICATIONS
        'murex', 'summit', 'calypso', 'sophis', 'reuters',
        'bloomberg', 'refinitiv', 'simcorp', 'aladdin',
        'front arena', 'kondor', 'kondor+', 'wall street',
        
        # RÉGULATION / REPORTING
        'mifid', 'basel', 'bâle', 'emir', 'dodd-frank',
        'regulatory reporting', 'reporting réglementaire',
        'solvency', 'solvabilité', 'capital', 'liquidity',
        
        # INSTRUMENTS FINANCIERS
        'bond', 'obligation', 'stock', 'action', 'share',
        'warrant', 'certificate', 'structured product',
        'produit structuré', 'exotic', 'vanilla',
        
        # OPERATIONS / POST-TRADE
        'operations', 'opérations', 'post-trade', 'post-marché',
        'clearing', 'compensation', 'custody', 'conservation',
        'back office', 'corporate actions', 'événements',
        
        # TREASURY / CASH
        'treasury', 'trésorerie', 'cash management', 'gestion de trésorerie',
        'liquidity', 'liquidité', 'funding', 'financement',
        'payment', 'paiement', 'swift', 'sepa',
        
        # ASSET SERVICING
        'transfer agent', 'agent de transfert', 'nav', 'valeur liquidative',
        'unit', 'part', 'subscription', 'souscription',
        'redemption', 'rachat', 'distribution', 'dividend',
        
        # PERFORMANCE / ANALYTICS
        'performance', 'attribution', 'benchmark', 'indice',
        'tracking', 'suivi', 'monitoring', 'surveillance',
        'dashboard', 'tableau de bord', 'kpi', 'reporting',
        
        # SECURITIES / INSTRUMENTS
        'securities', 'titres', 'financial instrument',
        'instrument financier', 'asset class', 'classe d\'actifs',
        'alternative', 'alternatif', 'real estate', 'immobilier',
        
        # CREDIT / LOANS
        'credit', 'crédit', 'loan', 'prêt', 'lending',
        'syndication', 'structured finance', 'finance structurée',
        'leveraged finance', 'corporate lending',
        
        # ESG / SUSTAINABLE
        'esg', 'sustainable', 'durable', 'green', 'vert',
        'sri', 'isr', 'responsible investment', 'impact',
        
        # CLIENT / DISTRIBUTION
        'client', 'customer', 'distribution', 'onboarding',
        'kyc', 'know your customer', 'aml', 'anti money laundering',
        'blanchiment', 'due diligence',
        
        # APPLICATIONS MÉTIER
        'pms', 'portfolio management system', 'oms', 'order management',
        'ems', 'execution management', 'rms', 'risk management system',
        'accounting', 'comptabilité', 'general ledger', 'grand livre',
        
        # AUTRES FINANCE/IT
        'financial data', 'données financières', 'market data feed',
        'flux de marché', 'tick', 'quote', 'cotation',
        'book', 'carnet d\'ordres', 'matching engine',
    ]
}


def _env(name, default):
    value = os.getenv(name)
    return value if value not in (None, '') else default


EMAIL_CONFIG = {
    'from': _env('EMAIL_FROM', 'loicjiraud@gmail.com'),
    'to': _env('EMAIL_TO', 'loicjiraud@gmail.com'),
    'password': os.getenv('EMAIL_PASSWORD', ''),
    'smtp_server': _env('SMTP_SERVER', 'smtp.gmail.com'),
    'smtp_port': int(_env('SMTP_PORT', '587'))
}

# ============================================
# DATABASE
# ============================================

def init_database():
    print(f"[DEBUG] BDD : {os.path.abspath(DB_FILE)}")
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # Table SANS contrainte URL, avec clé composite titre+entreprise+lieu
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS offres (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            titre TEXT NOT NULL,
            entreprise TEXT,
            lieu TEXT,
            date_trouvee TEXT,
            date_insertion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(titre, entreprise, lieu)
        )
    ''')
    conn.commit()
    conn.close()
    print(f"✅ BDD OK ({os.path.getsize(DB_FILE)} octets)\n")


def offre_existe(titre, entreprise, lieu):
    """Vérifie si l'offre existe sur la base titre+entreprise+lieu"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        SELECT COUNT(*) FROM offres 
        WHERE titre = ? AND entreprise = ? AND lieu = ?
    ''', (titre, entreprise, lieu))
    count = cursor.fetchone()[0]
    conn.close()
    return count > 0


def inserer_offre(offre):
    """Insère une offre dans la base"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    try:
        cursor.execute('''
            INSERT INTO offres (titre, entreprise, lieu, date_trouvee)
            VALUES (?, ?, ?, ?)
        ''', (offre['titre'], offre['entreprise'], offre['lieu'], offre['date']))
        conn.commit()
        print(f"  💾 Insérée")
    except sqlite3.IntegrityError:
        print(f"  ⚠️ Doublon")
    except Exception as e:
        print(f"  ❌ Erreur : {e}")
    finally:
        conn.close()


def get_stats():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM offres')
    total = cursor.fetchone()[0]
    conn.close()
    return total


def affiche_bdd_sample():
    print("\n[DEBUG] Dernières offres en base :")
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('SELECT titre, entreprise, lieu FROM offres ORDER BY id DESC LIMIT 5')
    rows = cursor.fetchall()
    if len(rows) == 0:
        print("  ❌ Base vide")
    else:
        for i, row in enumerate(rows, 1):
            print(f"  {i}. {row[0][:40]} | {row[1][:30]} | {row[2]}")
    conn.close()

# ============================================
# SCRAPING
# ============================================

def scraper_offres_vie():
    print("🚀 Scraping...")
    
    with sync_playwright() as p:
        headless = os.getenv('HEADLESS', 'true').lower() not in {'0', 'false', 'no'}
        browser = p.chromium.launch(headless=headless)
        page = browser.new_page()
        # page.goto('https://mon-vie-via.businessfrance.fr/offres/recherche?latest=true', timeout=60000)
        page.goto('https://mon-vie-via.businessfrance.fr/offres/recherche?query&specializationsIds=212&specializationsIds=24&missionsTypesIds=VIE&teletravail=0&porteEnv=0', timeout=60000)
        time.sleep(5)
        
        # Fermer popup cookies
        try:
            page.query_selector('button#didomi-notice-agree-button').click()
            time.sleep(2)
        except:
            pass
        
        # Nombre total
        try:
            total = int(''.join(filter(str.isdigit, page.query_selector('.count').inner_text())))
            print(f"🎯 Total : {total} offres")
        except:
            total = 9999
        
        click_more_count = 0
        while True:
            elements = page.query_selector_all('.figure_container')
            print(f"📊 Chargées : {len(elements)}/{total}")
            
            if len(elements) >= total:
                break
            
            try:
                btn = page.query_selector('a.btn_bleu_vert.see-more-btn')
                if btn and btn.is_visible():
                    if click_more_count >= 3:
                        print("🛑 Limite atteinte : 3 clics sur 'more'")
                        break
                    btn.scroll_into_view_if_needed()
                    time.sleep(1)
                    btn.click(force=True)
                    time.sleep(1)
                    click_more_count += 1
                else:
                    break
            except:
                break
        
        # Extraire
        elements = page.query_selector_all('.figure_container')
        print(f"✅ Total : {len(elements)} offres\n")
        
        offres = []
        for el in elements:
            try:
                titre_el = el.query_selector('h2')
                titre = titre_el.inner_text().strip() if titre_el else 'N/A'
                
                entreprise_el = el.query_selector('h3.organization')
                entreprise = entreprise_el.inner_text().strip() if entreprise_el else 'N/A'
                
                lieu_el = el.query_selector('.location')
                lieu = lieu_el.inner_text().strip() if lieu_el else 'N/A'
                
                offres.append({
                    'titre': titre,
                    'entreprise': entreprise,
                    'lieu': lieu,
                    'date': datetime.now().strftime('%Y-%m-%d')
                })
            except:
                continue
        
        browser.close()
        return offres


def filtrer_offres(offres):
    print(f"🔍 Filtrage de {len(offres)} offres...\n")
    filtrees = []
    
    for offre in offres:
        if any(kw in offre['titre'].lower() for kw in CRITERES['keywords']):
            filtrees.append(offre)
            print(f"✅ {offre['titre'][:60]} | {offre['entreprise'][:30]}")
    
    print(f"\n📊 {len(filtrees)} matchent\n")
    return filtrees


def filtrer_nouvelles_offres(offres):
    print(f"🔍 Vérification doublons...\n")
    nouvelles = []
    
    for offre in offres:
        if not offre_existe(offre['titre'], offre['entreprise'], offre['lieu']):
            nouvelles.append(offre)
            print(f"🆕 {offre['titre'][:60]}")
            inserer_offre(offre)
        else:
            print(f"⏭️ {offre['titre'][:60]}")
    
    print(f"\n📊 {len(nouvelles)} NOUVELLE(S)\n")
    return nouvelles


def envoyer_email(offres):
    if len(offres) == 0:
        print("📧 Pas de nouvelles offres\n")
        return
    
    print(f"📧 Envoi email ({len(offres)} offres)...")
    
    html = f"""<html><body style="font-family:Arial;max-width:800px;margin:0 auto;">
    <div style="background:linear-gradient(135deg,#667eea,#764ba2);color:white;padding:30px;text-align:center;border-radius:10px 10px 0 0;">
        <h1>🎯 {len(offres)} nouvelle(s) offre(s) VIE</h1>
        <p>{datetime.now().strftime('%d/%m/%Y à %H:%M')}</p>
    </div>
    <div style="padding:20px;background:#f7f7f7;">
        {''.join([f'<div style="margin:20px 0;padding:20px;background:white;border-left:4px solid #667eea;border-radius:8px;"><h3 style="color:#667eea;margin:0;">{i+1}. {o["titre"]}</h3><p><strong>🏢</strong> {o["entreprise"]}<br><strong>📍</strong> {o["lieu"]}</p></div>' for i,o in enumerate(offres)])}
    </div>
    <div style="text-align:center;padding:20px;color:#888;"><p>🤖 Scraper VIE automatique</p></div>
    </body></html>"""
    
    msg = MIMEMultipart('alternative')
    msg['Subject'] = f"🎯 {len(offres)} nouvelle(s) offre(s) VIE - {datetime.now().strftime('%d/%m/%Y')}"
    msg['From'] = EMAIL_CONFIG['from']
    msg['To'] = EMAIL_CONFIG['to']
    msg.attach(MIMEText(html, 'html'))
    
    try:
        srv = smtplib.SMTP(EMAIL_CONFIG['smtp_server'], EMAIL_CONFIG['smtp_port'])
        srv.starttls()
        srv.login(EMAIL_CONFIG['from'], EMAIL_CONFIG['password'])
        srv.send_message(msg)
        srv.quit()
        print("✅ Email envoyé !\n")
    except Exception as e:
        print(f"❌ Erreur : {e}\n")

# ============================================
# MAIN
# ============================================

if __name__ == "__main__":
    print("\n" + "="*60)
    print("🎯 SCRAPER VIE - SANS URL")
    print("="*60 + "\n")
    
    init_database()
    print(f"📊 Base AVANT : {get_stats()} offres")
    affiche_bdd_sample()
    
    offres = scraper_offres_vie()
    offres_filtrees = filtrer_offres(offres)
    nouvelles = filtrer_nouvelles_offres(offres_filtrees)
    envoyer_email(nouvelles)
    
    print(f"📊 Base APRÈS : {get_stats()} offres")
    affiche_bdd_sample()
    
    print("\n" + "="*60)
    print("✅ Terminé !")
    print("="*60 + "\n")
