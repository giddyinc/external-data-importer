import braintree

gateway = braintree.BraintreeGateway(
  braintree.Configuration(
    environment=braintree.Environment.Sandbox,
    merchant_id='....',
    public_key='....',
    private_key='.......'
  )
)

results = gateway.transaction.search(braintree.TransactionSearch.status == "settled")
print (results)
for transaction in results:
    print (transaction.id)
    print (transaction)
    print ("------------")

print ("Script Complete")
