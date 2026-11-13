package alerting

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

// alerting service, no cool name :(
type Alerter struct {
	shared.CatalogPool
	otelManager *otel_metrics.OtelManager
	// normalizeGapLogged is when each flow last logged its normalize gap, to log it once per alerting gap
	normalizeGapLogged sync.Map
	// noProgressLogged is when each flow last logged that it makes no progress, to log it once per alerting gap
	noProgressLogged sync.Map
}

type AlertSenderConfig struct {
	Sender          AlertSender
	AlertForMirrors []string
	Id              int64
}

type AlertKeys struct {
	FlowName string
	PeerName string
	SlotName string
	// PendingBatches: the interval is how long synced batches have waited for normalize (non-Postgres sources),
	// so an idle source can't explain it
	PendingBatches bool
}

// doesn't take care of closing pool, needs to be done externally.
func NewAlerter(_ context.Context, catalogPool shared.CatalogPool, otelManager *otel_metrics.OtelManager) *Alerter {
	if catalogPool.Pool == nil {
		panic("catalog pool is nil for Alerter")
	}
	return &Alerter{
		CatalogPool: catalogPool,
		otelManager: otelManager,
	}
}

func (a *Alerter) registerSendersFromPool(ctx context.Context) ([]AlertSenderConfig, error) {
	rows, err := a.CatalogPool.Query(ctx,
		`SELECT id, service_type, service_config, enc_key_id, alert_for_mirrors
		FROM peerdb_stats.alerting_config`)
	if err != nil {
		return nil, fmt.Errorf("failed to read alerter config from catalog: %w", err)
	}

	keys := internal.PeerDBEncKeys(ctx)
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (AlertSenderConfig, error) {
		var alertSenderConfig AlertSenderConfig
		var serviceType ServiceType
		var serviceConfigEnc []byte
		var encKeyId string
		if err := row.Scan(&alertSenderConfig.Id, &serviceType, &serviceConfigEnc, &encKeyId,
			&alertSenderConfig.AlertForMirrors); err != nil {
			return alertSenderConfig, err
		}

		key, err := keys.Get(encKeyId)
		if err != nil {
			return alertSenderConfig, err
		}
		serviceConfig, err := key.Decrypt(serviceConfigEnc)
		if err != nil {
			return alertSenderConfig, err
		}

		switch serviceType {
		case SLACK:
			var slackServiceConfig slackAlertConfig
			if err := json.Unmarshal(serviceConfig, &slackServiceConfig); err != nil {
				return alertSenderConfig, fmt.Errorf("failed to unmarshal %s service config: %w", serviceType, err)
			}

			alertSenderConfig.Sender = newSlackAlertSender(&slackServiceConfig)
			return alertSenderConfig, nil
		case EMAIL:
			var replyToAddresses []string
			if replyToEnvString := strings.TrimSpace(
				internal.PeerDBAlertingEmailSenderReplyToAddresses()); replyToEnvString != "" {
				replyToAddresses = strings.Split(replyToEnvString, ",")
			}
			emailServiceConfig := EmailAlertSenderConfig{
				sourceEmail:          internal.PeerDBAlertingEmailSenderSourceEmail(),
				configurationSetName: internal.PeerDBAlertingEmailSenderConfigurationSet(),
				replyToAddresses:     replyToAddresses,
			}
			if emailServiceConfig.sourceEmail == "" {
				return alertSenderConfig, fmt.Errorf("missing sourceEmail for Email alerting service")
			}
			if err := json.Unmarshal(serviceConfig, &emailServiceConfig); err != nil {
				return alertSenderConfig, fmt.Errorf("failed to unmarshal %s service config: %w", serviceType, err)
			}
			var region *string
			if envRegion := internal.PeerDBAlertingEmailSenderRegion(); envRegion != "" {
				region = &envRegion
			}

			alertSender, alertSenderErr := NewEmailAlertSenderWithNewClient(ctx, region, &emailServiceConfig)
			if alertSenderErr != nil {
				return AlertSenderConfig{}, fmt.Errorf("failed to initialize email alerter: %w", alertSenderErr)
			}
			alertSenderConfig.Sender = alertSender

			return alertSenderConfig, nil
		default:
			return alertSenderConfig, fmt.Errorf("unknown service type: %s", serviceType)
		}
	})
}

func (a *Alerter) AlertIfSlotLag(ctx context.Context, alertKeys *AlertKeys, slotInfo *protos.SlotInfo) {
	alertSenderConfigs, err := a.registerSendersFromPool(ctx)
	if err != nil {
		internal.LoggerFromCtx(ctx).Warn("failed to set alert senders", slog.Any("error", err))
		return
	}

	deploymentUIDPrefix := ""
	if internal.PeerDBDeploymentUID() != "" {
		deploymentUIDPrefix = fmt.Sprintf("[%s] ", internal.PeerDBDeploymentUID())
	}

	defaultSlotLagMBAlertThreshold, err := internal.PeerDBSlotLagMBAlertThreshold(ctx, nil)
	if err != nil {
		internal.LoggerFromCtx(ctx).Warn("failed to get slot lag alert threshold from catalog", slog.Any("error", err))
		return
	}

	// catalog cannot use default threshold to space alerts properly, use the lowest set threshold instead
	lowestSlotLagMBAlertThreshold := defaultSlotLagMBAlertThreshold
	var alertSendersForMirrors []AlertSenderConfig
	for _, alertSenderConfig := range alertSenderConfigs {
		if len(alertSenderConfig.AlertForMirrors) == 0 || slices.Contains(alertSenderConfig.AlertForMirrors, alertKeys.FlowName) {
			alertSendersForMirrors = append(alertSendersForMirrors, alertSenderConfig)
			if alertSenderConfig.Sender.getSlotLagMBAlertThreshold() > 0 {
				lowestSlotLagMBAlertThreshold = min(lowestSlotLagMBAlertThreshold, alertSenderConfig.Sender.getSlotLagMBAlertThreshold())
			}
		}
	}

	thresholdAlertKey := fmt.Sprintf("%s Slot Lag Threshold Exceeded for Peer %s", deploymentUIDPrefix, alertKeys.PeerName)
	thresholdAlertMessageTemplate := fmt.Sprintf("%sSlot `%s` on peer `%s` has exceeded threshold size of %%dMB, "+
		`currently at %.2fMB!`, deploymentUIDPrefix, slotInfo.SlotName, alertKeys.PeerName, slotInfo.LagInMb)

	badWalStatusAlertKey := fmt.Sprintf("%s Bad WAL Status for Peer %s", deploymentUIDPrefix, alertKeys.PeerName)
	badWalStatusAlertMessage := fmt.Sprintf("%sSlot `%s` on peer `%s` has bad WAL status: `%s`",
		deploymentUIDPrefix, slotInfo.SlotName, alertKeys.PeerName, slotInfo.WalStatus)

	for _, alertSenderConfig := range alertSendersForMirrors {
		if a.checkAndAddAlertToCatalog(ctx,
			alertSenderConfig.Id, thresholdAlertKey,
			fmt.Sprintf(thresholdAlertMessageTemplate, lowestSlotLagMBAlertThreshold)) {
			if alertSenderConfig.Sender.getSlotLagMBAlertThreshold() > 0 {
				if slotInfo.LagInMb > float32(alertSenderConfig.Sender.getSlotLagMBAlertThreshold()) {
					a.alertToProvider(ctx, alertSenderConfig, thresholdAlertKey,
						fmt.Sprintf(thresholdAlertMessageTemplate, alertSenderConfig.Sender.getSlotLagMBAlertThreshold()))
				}
			} else {
				if slotInfo.LagInMb > float32(defaultSlotLagMBAlertThreshold) {
					a.alertToProvider(ctx, alertSenderConfig, thresholdAlertKey,
						fmt.Sprintf(thresholdAlertMessageTemplate, defaultSlotLagMBAlertThreshold))
				}
			}
		}

		if (slotInfo.WalStatus == "lost" || slotInfo.WalStatus == "unreserved") &&
			a.checkAndAddAlertToCatalog(ctx, alertSenderConfig.Id, badWalStatusAlertKey, badWalStatusAlertMessage) {
			a.alertToProvider(ctx, alertSenderConfig, badWalStatusAlertKey, badWalStatusAlertMessage)
		}
	}
}

func (a *Alerter) AlertIfOpenConnections(ctx context.Context, alertKeys *AlertKeys,
	openConnections *protos.GetOpenConnectionsForUserResult,
) {
	alertSenderConfigs, err := a.registerSendersFromPool(ctx)
	if err != nil {
		internal.LoggerFromCtx(ctx).Warn("failed to set alert senders", slog.Any("error", err))
		return
	}

	deploymentUIDPrefix := ""
	if internal.PeerDBDeploymentUID() != "" {
		deploymentUIDPrefix = fmt.Sprintf("[%s] - ", internal.PeerDBDeploymentUID())
	}

	// same as with slot lag, use lowest threshold for catalog
	defaultOpenConnectionsThreshold, err := internal.PeerDBOpenConnectionsAlertThreshold(ctx, nil)
	if err != nil {
		internal.LoggerFromCtx(ctx).Warn("failed to get open connections alert threshold from catalog", slog.Any("error", err))
		return
	}
	lowestOpenConnectionsThreshold := defaultOpenConnectionsThreshold
	for _, alertSender := range alertSenderConfigs {
		if alertSender.Sender.getOpenConnectionsAlertThreshold() > 0 {
			lowestOpenConnectionsThreshold = min(lowestOpenConnectionsThreshold,
				alertSender.Sender.getOpenConnectionsAlertThreshold())
		}
	}

	alertKey := fmt.Sprintf("%s Max Open Connections Threshold Exceeded for Peer %s", deploymentUIDPrefix, alertKeys.PeerName)
	alertMessageTemplate := fmt.Sprintf("%sOpen connections from PeerDB user `%s` on peer `%s`"+
		` has exceeded threshold size of %%d connections, currently at %d connections!`,
		deploymentUIDPrefix, openConnections.UserName, alertKeys.PeerName, openConnections.CurrentOpenConnections)

	if openConnections.CurrentOpenConnections > int64(lowestOpenConnectionsThreshold) {
		for _, alertSenderConfig := range alertSenderConfigs {
			if len(alertSenderConfig.AlertForMirrors) > 0 &&
				!slices.Contains(alertSenderConfig.AlertForMirrors, alertKeys.FlowName) {
				continue
			}
			if a.checkAndAddAlertToCatalog(ctx,
				alertSenderConfig.Id, alertKey, fmt.Sprintf(alertMessageTemplate, lowestOpenConnectionsThreshold)) {
				if alertSenderConfig.Sender.getOpenConnectionsAlertThreshold() > 0 {
					if openConnections.CurrentOpenConnections > int64(alertSenderConfig.Sender.getOpenConnectionsAlertThreshold()) {
						a.alertToProvider(ctx, alertSenderConfig, alertKey,
							fmt.Sprintf(alertMessageTemplate, alertSenderConfig.Sender.getOpenConnectionsAlertThreshold()))
					}
				} else {
					if openConnections.CurrentOpenConnections > int64(defaultOpenConnectionsThreshold) {
						a.alertToProvider(ctx, alertSenderConfig, alertKey,
							fmt.Sprintf(alertMessageTemplate, defaultOpenConnectionsThreshold))
					}
				}
			}
		}
	}
}

func (a *Alerter) AlertIfTooLongSinceLastNormalize(ctx context.Context, alertKeys *AlertKeys, intervalSinceLastNormalize time.Duration) {
	intervalSinceLastNormalizeThreshold, err := internal.PeerDBIntervalSinceLastNormalizeThresholdMinutes(ctx, nil)
	if err != nil {
		internal.LoggerFromCtx(ctx).
			Warn("failed to get interval since last normalize threshold from catalog", slog.Any("error", err))
	}

	if intervalSinceLastNormalizeThreshold == 0 {
		internal.LoggerFromCtx(ctx).Info("Alerting disabled via environment variable, returning")
		return
	}
	if intervalSinceLastNormalize <= time.Duration(intervalSinceLastNormalizeThreshold)*time.Minute {
		return
	}
	// batches waiting for normalize are logged whether or not alert senders are configured, so a stalled normalize
	// is never silent, but once per alerting gap like the alert: the check runs every minute. Not the Postgres
	// source's gap, which an idle source explains
	if logGap, err := internal.PeerDBAlertingGapMinutesAsDuration(ctx, nil); err == nil && alertKeys.PendingBatches &&
		a.normalizeGapDue(alertKeys.FlowName, logGap, time.Now()) {
		internal.LoggerFromCtx(ctx).Warn("too long since last data normalize", slog.String("flowName", alertKeys.FlowName),
			slog.Duration("intervalSinceLastNormalize", intervalSinceLastNormalize))
	}
	alertSenderConfigs, err := a.registerSendersFromPool(ctx)
	if err != nil {
		internal.LoggerFromCtx(ctx).Warn("failed to set alert senders", slog.Any("error", err))
		return
	}

	deploymentUIDPrefix := ""
	if internal.PeerDBDeploymentUID() != "" {
		deploymentUIDPrefix = fmt.Sprintf("[%s] - ", internal.PeerDBDeploymentUID())
	}

	alertKey := fmt.Sprintf("%s Too long since last data normalize for PeerDB mirror %s",
		deploymentUIDPrefix, alertKeys.FlowName)
	alertMessage := fmt.Sprintf("%sData hasn't been synced to the target for mirror `%s` since the last `%s`."+
		` This could indicate an issue with the pipeline — please check the UI and logs to confirm.`+
		` Alternatively, it might be that the source database is idle and not receiving new updates.`, deploymentUIDPrefix,
		alertKeys.FlowName, intervalSinceLastNormalize)
	if alertKeys.PendingBatches {
		alertMessage = fmt.Sprintf("%sData read from the source for mirror `%s` has been waiting `%s` to be applied to "+
			"the target. Normalize isn't keeping up or keeps failing — please check the mirror's errors in the UI and logs.",
			deploymentUIDPrefix, alertKeys.FlowName, intervalSinceLastNormalize)
	}

	for _, alertSenderConfig := range alertSenderConfigs {
		if len(alertSenderConfig.AlertForMirrors) == 0 ||
			slices.Contains(alertSenderConfig.AlertForMirrors, alertKeys.FlowName) {
			if a.checkAndAddAlertToCatalog(ctx, alertSenderConfig.Id, alertKey, alertMessage) {
				a.alertToProvider(ctx, alertSenderConfig, alertKey, alertMessage)
			}
		}
	}
}

// normalizeGapDue reports whether a flow's normalize-gap warning is due: once per gap (logDue).
func (a *Alerter) normalizeGapDue(flowName string, gap time.Duration, now time.Time) bool {
	return logDue(&a.normalizeGapLogged, flowName, gap, now)
}

// logDue reports whether a flow's warning kept in logged is due: once per gap. It forgets flows not logged for two
// gaps (dropped mirrors). Per worker process, so a restart or another worker may log it again.
func logDue(logged *sync.Map, flowName string, gap time.Duration, now time.Time) bool {
	if last, ok := logged.Load(flowName); ok && now.Sub(last.(time.Time)) < gap {
		return false
	}
	logged.Store(flowName, now)
	logged.Range(func(k, v any) bool {
		if now.Sub(v.(time.Time)) >= 2*gap {
			logged.Delete(k)
		}
		return true
	})
	return true
}

// alertIfNoProgress warns in the mirror's log and alerts, once per alerting gap, when an error that needs the user
// was just recorded for a flow that has been failing without progress for longer than
// PEERDB_NO_PROGRESS_ALERT_THRESHOLD_MINUTES: it also logged an error that long ago, after its last progress, so an
// idle or resumed mirror's first error doesn't alert. The errors in between can be of any class, and there's no exact
// "failing since" (the error log has the history). Not covered: errors classified ErrorOther, failures not recorded
// through the Alerter, and a mirror that goes FAILED before the threshold.
func (a *Alerter) alertIfNoProgress(ctx context.Context, flowName string, errClass ErrorClass, err error) {
	logger := internal.LoggerFromCtx(ctx)
	// snapshot partitions report their errors under their own clone_ flow; progress and the alert are the mirror's
	if metadata := internal.GetFlowMetadata(ctx); metadata != nil && metadata.FlowName != "" {
		flowName = metadata.FlowName
	}
	thresholdMinutes, confErr := internal.PeerDBNoProgressAlertThresholdMinutes(ctx, nil)
	if confErr != nil {
		logger.Warn("failed to get the no-progress alert threshold from catalog", slog.Any("error", confErr))
		return
	}
	if thresholdMinutes == 0 {
		return
	}
	progress, progressErr := internal.GetFlowProgress(ctx, a.CatalogPool, flowName, time.Duration(thresholdMinutes)*time.Minute)
	if progressErr != nil {
		if !errors.Is(progressErr, pgx.ErrNoRows) {
			logger.Warn("failed to check the mirror's progress", slog.Any("error", progressErr))
		}
		return
	}
	if !progress.FailingWithoutProgress {
		return
	}
	gap, gapErr := internal.PeerDBAlertingGapMinutesAsDuration(ctx, nil)
	if gapErr != nil || gap == 0 {
		gap = 15 * time.Minute
	}
	if !logDue(&a.noProgressLogged, flowName, gap, time.Now()) {
		return
	}

	firstLine, _, _ := strings.Cut(err.Error(), "\n")
	if len(firstLine) > 500 {
		firstLine = strings.ToValidUTF8(firstLine[:500], "") + "…"
	}
	message := fmt.Sprintf("Mirror %s has made no progress since %s (%d minutes ago) and keeps failing with "+
		"errors that need a fix (%s): %s", flowName, progress.LastProgress.UTC().Format(time.RFC3339),
		int64(progress.Since.Minutes()), errClass.String(), firstLine)
	logger.Warn(message, slog.String("flowName", flowName))
	if logErr := InsertFlowLog(ctx, a.CatalogPool, flowName, message, FlowErrorTypeWarn); logErr != nil {
		logger.Warn("failed to insert flow warning", slog.Any("error", logErr))
	}

	alertSenderConfigs, sendersErr := a.registerSendersFromPool(ctx)
	if sendersErr != nil {
		logger.Warn("failed to set alert senders", slog.Any("error", sendersErr))
		return
	}
	deploymentUIDPrefix := ""
	if internal.PeerDBDeploymentUID() != "" {
		deploymentUIDPrefix = fmt.Sprintf("[%s] - ", internal.PeerDBDeploymentUID())
	}
	alertKey := fmt.Sprintf("%s No progress with errors that need a fix for PeerDB mirror %s", deploymentUIDPrefix, flowName)
	alertMessage := deploymentUIDPrefix + message + " — please check the mirror's errors in the UI."
	for _, alertSenderConfig := range alertSenderConfigs {
		if len(alertSenderConfig.AlertForMirrors) == 0 || slices.Contains(alertSenderConfig.AlertForMirrors, flowName) {
			if a.checkAndAddAlertToCatalog(ctx, alertSenderConfig.Id, alertKey, alertMessage) {
				a.alertToProvider(ctx, alertSenderConfig, alertKey, alertMessage)
			}
		}
	}
}

func (a *Alerter) alertToProvider(ctx context.Context, alertSenderConfig AlertSenderConfig, alertKey string, alertMessage string) {
	if err := alertSenderConfig.Sender.sendAlert(ctx, alertKey, alertMessage); err != nil {
		internal.LoggerFromCtx(ctx).Warn("failed to send alert", slog.Any("error", err))
	}
}

// Only raises an alert if another alert with the same key hasn't been raised
// in the past X minutes, where X is configurable and defaults to 15 minutes
// returns true if alert added to catalog, so proceed with processing alerts to slack
func (a *Alerter) checkAndAddAlertToCatalog(ctx context.Context, alertConfigId int64, alertKey string, alertMessage string) bool {
	logger := internal.LoggerFromCtx(ctx)
	dur, err := internal.PeerDBAlertingGapMinutesAsDuration(ctx, nil)
	if err != nil {
		logger.Warn("failed to get alerting gap duration from catalog", slog.Any("error", err))
		return false
	}
	if dur == 0 {
		logger.Warn("Alerting disabled via environment variable, returning")
		return false
	}

	var createdTimestamp time.Time
	if err := a.CatalogPool.QueryRow(ctx,
		`SELECT created_timestamp FROM peerdb_stats.alerts_v1 WHERE alert_key=$1 AND alert_config_id=$2
		 ORDER BY created_timestamp DESC LIMIT 1`,
		alertKey, alertConfigId,
	).Scan(&createdTimestamp); err != nil && !errors.Is(err, pgx.ErrNoRows) {
		internal.LoggerFromCtx(ctx).Warn("failed to send alert", slog.Any("err", err))
		return false
	}

	if time.Since(createdTimestamp) >= dur {
		if _, err := a.CatalogPool.Exec(ctx,
			"INSERT INTO peerdb_stats.alerts_v1(alert_key,alert_message,alert_config_id) VALUES($1,$2,$3)",
			alertKey, alertMessage, alertConfigId,
		); err != nil {
			internal.LoggerFromCtx(ctx).Warn("failed to insert alert", slog.Any("error", err))
			return false
		}
		return true
	}

	logger.Info(fmt.Sprintf("Skipped sending alerts: last alert was sent at %s, which was <=%s ago", createdTimestamp.String(), dur.String()))
	return false
}

type FlowErrorType string

func (f FlowErrorType) String() string {
	return string(f)
}

const (
	FlowErrorTypeInfo  FlowErrorType = "info"
	FlowErrorTypeWarn  FlowErrorType = "warn"
	FlowErrorTypeError FlowErrorType = "error"
)

// recordFlowErrorInternal pushes the error to the errors table and emits a metric.
func (a *Alerter) recordFlowErrorInternal(
	ctx context.Context,
	flowName string,
	errorType FlowErrorType,
	err error,
) {
	logger := internal.LoggerFromCtx(ctx)
	errClass, errInfo := GetErrorClass(ctx, err)
	errMessage := fmt.Sprintf("%+v", err)

	// 1. Log the error internally
	logFn := logger.Error
	if errorType == FlowErrorTypeWarn {
		logFn = logger.Warn
	}
	logFn(err.Error(),
		slog.Any("errorClass", errClass),
		slog.Any("errorInfo", errInfo),
		slog.Any("errorMessage", errMessage),
		slog.String("flowErrorType", errorType.String()),
	)

	if ctx.Err() != nil {
		logger.Error("Skipping flow error handling: " + ctx.Err().Error())
		return
	}

	// 2. Insert log to flow_errors table
	if err := InsertFlowLog(ctx, a.CatalogPool, flowName, errMessage, errorType); err != nil {
		logger.Error("failed to insert flow error", slog.Any("error", err))
	}
	if errorType == FlowErrorTypeError && errClass.ErrorAction() == NotifyUser {
		a.alertIfNoProgress(ctx, flowName, errClass, err)
	}

	// 3. Record error metrics
	errorAttributes := []attribute.KeyValue{
		attribute.Stringer(otel_metrics.ErrorClassKey, errClass),
		attribute.Stringer(otel_metrics.ErrorActionKey, errClass.ErrorAction()),
		attribute.Stringer(otel_metrics.ErrorSourceKey, errInfo.Source),
		attribute.String(otel_metrics.ErrorCodeKey, errInfo.Code),
	}
	if len(errInfo.AdditionalAttributes) != 0 {
		for k, v := range errInfo.AdditionalAttributes {
			errorAttributes = append(errorAttributes, attribute.String(k.String(), v))
		}
	}
	errorAttributeSet := metric.WithAttributeSet(attribute.NewSet(errorAttributes...))
	counter := a.otelManager.Metrics.ErrorsEmittedCounter
	gauge := a.otelManager.Metrics.ErrorEmittedGauge
	if errorType == FlowErrorTypeWarn {
		counter = a.otelManager.Metrics.WarningEmittedCounter
		gauge = a.otelManager.Metrics.WarningsEmittedGauge
	}
	counter.Add(ctx, 1, errorAttributeSet)
	gauge.Record(ctx, 1, errorAttributeSet)
}

func (a *Alerter) LogFlowError(ctx context.Context, flowName string, err error) error {
	a.recordFlowErrorInternal(ctx, flowName, FlowErrorTypeError, err)
	return err
}

func (a *Alerter) LogFlowWarning(ctx context.Context, flowName string, err error) {
	a.recordFlowErrorInternal(ctx, flowName, FlowErrorTypeWarn, err)
}

func (a *Alerter) LogFlowInfo(ctx context.Context, flowName string, message string) {
	if err := InsertFlowLog(ctx, a.CatalogPool, flowName, message, FlowErrorTypeInfo); err != nil {
		logger := internal.SlogLoggerFromCtx(ctx)
		logger.WarnContext(ctx, "failed to insert flow info", slog.Any("error", err))
	}
}

// InsertFlowLog Historically flow_errors table only stored errors, hence the name.
// It has since evolved to surface all user-facing logs.
func InsertFlowLog(
	ctx context.Context,
	catalogPool shared.CatalogPool,
	flowName string,
	message string,
	errorType FlowErrorType,
) error {
	logger := internal.SlogLoggerFromCtx(ctx)
	logger.InfoContext(ctx, fmt.Sprintf("inserting user-facing flow log: [%s] %s", errorType.String(), message),
		slog.String("flowName", flowName),
		slog.String("severity", errorType.String()),
		slog.String("message", message),
		slog.String("logAudience", "user"),
	)
	_, err := catalogPool.Exec(
		ctx, "INSERT INTO peerdb_stats.flow_errors(flow_name,error_message,error_type) VALUES($1,$2,$3)",
		flowName, message, errorType.String(),
	)
	return err
}
