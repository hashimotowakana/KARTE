import boto3
import json
import os
from datetime import datetime, timezone, timedelta

# Amazon Connectクライアントの設定
client = boto3.client('connect', region_name=os.environ['REGION'])

def lambda_handler(event, context):
    # イベント情報のログ出力
    print("Event: ", json.dumps(event))
    records = event['Records']
    for record in records:
        # SNSメッセージの抽出とログ出力
        sns_message = record['Sns']['Message']
        print("SNS Message: ", sns_message)
        content = json.loads(sns_message)['detail']['content']

        # 必須フィールドのチェック
        if not 'phone_e164' in content:
            print('Necessary fields are missed')
            return {
                'statusCode': 400,
                'body': 'Necessary fields are missed'
            }
        # タスク作成関数の呼び出し
        createTask(content)

def createTask(content):
    try:
        # 日本標準時(JST)へのタイムゾーン変換
        JST = timezone(timedelta(hours=+9), 'JST')
        epoch_time = int(content["date"])
        time = datetime.fromtimestamp(epoch_time).replace(tzinfo=timezone.utc).astimezone(tz=JST)
        time = time.strftime('%Y-%m-%d %H:%M:%S')
        
        # ユーザーIDのフォーマット
        user_id = content["user_id"]
        formatted_user_id = "{}-{}-{}".format(user_id[:4], user_id[4:6], user_id[6:])
        
        # Amazon Connectでのタスクの作成
        response = client.start_task_contact(
            InstanceId=os.environ['INSTANCE_ID'],
            ContactFlowId=os.environ['CONTACT_FLOW_ID'],
            Attributes={
                "inquiry_date": time,
                "source": content["source"],
                "phone_e164": content["phone_e164"],
                "page_title": content["page_title"],
                "user_id": formatted_user_id
            },
            Name='CallBack Task',
            Description='HPからの問い合わせ'
        )
        # タスク作成成功のログ出力
        print("Task successfully created: ", response)
        return {
            'statusCode': 200,
            'body': 'Task successfully created'
        }
    except Exception as e:
        # エラーログの出力
        print("Error: ", e)
        return {
            'statusCode': 500,
            'body': str(e)
        }
