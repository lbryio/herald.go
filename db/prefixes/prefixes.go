package prefixes

const (
	ClaimToSupport = 'K'
	SupportToClaim = 'L'

	ClaimToTXO = 'E'
	TXOToClaim = 'G'

	ClaimToChannel = 'I'
	ChannelToClaim = 'J'

	ClaimShortIdPrefix = 'F'
	EffectiveAmount    = 'D'
	ClaimExpiration    = 'O'

	ActiveAmount             = 'S'
	ClaimTakeover            = 'P'
	PendingActivation        = 'Q'
	ActivatedClaimAndSupport = 'R'

	Repost        = 'V'
	RepostedClaim = 'W'

	Undo      = 'M'
	ClaimDiff = 'Y'

	Tx            = 'B'
	BlockHash     = 'C'
	Header        = 'H'
	TxNum         = 'N'
	TxCount       = 'T'
	TxHash        = 'X'
	UTXO          = 'u'
	HashXUTXO     = 'h'
	HashXHistory  = 'x'
	DBState       = 's'
	ChannelCount  = 'Z'
	SupportAmount = 'a'
	BlockTXs      = 'b'
)
