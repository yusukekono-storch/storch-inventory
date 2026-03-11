# シュトルヒ在庫管理システム

約200 SKUの在庫をリアルタイムに把握し、需要予測に基づいてメーカー発注数・FBA補充数を自動算出するWebアプリケーション。

## セットアップ

```bash
pip install -r requirements.txt
streamlit run app.py
```

## 初回データインポート

1. 設定ページから「商品マスター」CSVをアップロード
2. 「カインズ在庫」CSVをアップロード
3. 「RSL在庫」CSVをアップロード
4. 「NextEngine受注データ」CSVをアップロード

## デプロイ

Streamlit Community Cloudでの自動デプロイに対応。
GitHubリポジトリにpushするだけでデプロイされます。
