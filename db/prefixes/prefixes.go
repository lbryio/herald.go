package prefixes

var (
	ClaimToSupport = []byte("K")
	SupportToClaim = []byte("L")

	ClaimToTXO = []byte("E")
	TXOToClaim = []byte("G")

	ClaimToChannel = []byte("I")
	ChannelToClaim = []byte("J")

	ClaimShortIdPrefix = []byte("F")
	EffectiveAmount    = []byte("D")
	ClaimExpiration    = []byte("O")

	ClaimTakeover            = []byte("P")
	PendingActivation        = []byte("Q")
	ActivatedClaimAndSupport = []byte("R")
	ActiveAmount             = []byte("S")

	Repost        = []byte("V")
	RepostedClaim = []byte("W")

	Undo      = []byte("M")
	ClaimDiff = []byte("Y")

	Tx            = []byte("B")
	BlockHash     = []byte("C")
	Header        = []byte("H")
	TxNum         = []byte("N")
	TxCount       = []byte("T")
	TxHash        = []byte("X")
	UTXO          = []byte("u")
	HashXUTXO     = []byte("h")
	HashXHistory  = []byte("x")
	DBState       = []byte("s")
	ChannelCount  = []byte("Z")
	SupportAmount = []byte("a")
	BlockTXs      = []byte("b")
)
