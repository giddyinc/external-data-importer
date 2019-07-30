import braintree

gateway = braintree.BraintreeGateway(
  braintree.Configuration(
    environment=braintree.Environment.Sandbox,
    merchant_id='ywkk3y9tjttb8s4h',
    public_key='y98zgwhtr629sxw7',
    private_key='01ed02c4f687dde7abaf96aa29b7cc6b'
  )
)

results = gateway.transaction.search(braintree.TransactionSearch.status == "settled")
print (results)
for transaction in results:
    print (transaction.id)
    print (transaction)
    print ("------------")

print ("Script Complete")
