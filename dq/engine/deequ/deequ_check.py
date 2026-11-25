# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

from pyspark.sql import SparkSession
from pydeequ.checks import Check, CheckLevel, ConstrainableDataTypes
from pyhocon import ConfigTree
from pydeequ.verification import VerificationSuite

from dq.engine import checkspec
from dq.engine.deequ.expression import parse_assertion


class DeequCheck:
    """Builds and applies PyDeequ Check constraints from HOCON configuration.

    Supports both single-check mode (all constraints chained on one Check)
    and multi-check mode (one Check per constraint for granular reporting).

    Per-rule severity crosses the shared check seam: ``level`` accepts the
    Deequ spellings (``Error``/``Warning``) and the canonical kernel
    spellings (``error``/``warn``) alike, normalized fail-closed.

    Each check's label is its configured name -- ``constraint_name``,
    ``alias`` or ``description`` -- or else its constraint and columns, as
    :func:`dq.engine.checkspec.labels_for_checks` decides. Two checks that
    would report the same label are refused: their evidence could not be
    told apart.
    """

    _check_level_map = {"error": CheckLevel.Error, "warn": CheckLevel.Warning}
    _name_keys = ("constraint_name", "alias", "description")
    single_check_description = "Deequ constraints"

    def __init__(self, checks_config: ConfigTree, single_check_mode=False):
        self._checks_config = checks_config
        self._single_check_mode = single_check_mode
        self._labels = {}
        self._order = []

    def labels(self) -> list:
        """Return every check's label in configuration order, repeats included."""
        return list(self._order)

    def labels_by_check(self) -> dict:
        """Return ``{Check description: [label, ...]}`` in constraint order.

        Result rows are matched to these by their Check's description and
        their position among that Check's rows.
        """
        return {
            description: list(labels) for description, labels in self._labels.items()
        }

    def apply_checks(
        self, verification_run_builder: VerificationSuite, spark_session: SparkSession
    ):
        if self._single_check_mode:
            check = Check(
                spark_session=spark_session,
                level=CheckLevel.Error,
                description=self.single_check_description,
            )
            self._labels = {self.single_check_description: []}
        else:
            check_list = []
            self._labels = {}
        self._order = []

        # One labelling rule for the whole rule: a check with no columns is
        # named by its constraint alone unless an earlier check shares it.
        labels = checkspec.labels_for_checks(
            self._checks_config, self._name_keys, "constraint", ("column", "columns")
        )
        for index, check_config in enumerate(self._checks_config):
            columns = check_config.get("columns", None)
            column = check_config.get("column", None)
            constraint = check_config.get("constraint", None)
            assertion = check_config.get("assertion", None)
            hint = check_config.get("hint", None)
            kwargs = check_config.get("kwargs", None)
            label = labels[index]
            if self._single_check_mode:
                # Constraints share a Check here; the single Check's level
                # is Error and each constraint's own severity is reported
                # from its result row.
                self._labels[self.single_check_description].append(label)
                self._order.append(label)
            else:
                criticality = checkspec.normalize_criticality(
                    check_config.get("level", None)
                )
                check = Check(
                    spark_session=spark_session,
                    level=self._check_level_map[criticality],
                    description=label,
                )
                # Kept in a list keyed by description: two checks may share
                # a label until the uniqueness check below refuses them.
                self._labels.setdefault(label, []).append(label)
                self._order.append(label)
            # Apply constraint dynamically
            # Refer following githb link for complete list of supported python-deequ checks.
            # https://github.com/awslabs/python-deequ/blob/master/docs/checks.md

            method = getattr(check, constraint)
            if assertion:
                assertion_func = parse_assertion(assertion)
            else:
                assertion_func = None

            if method:
                constrainedCheck = None

                if "hasDataType" == constraint:
                    datatype = check_config.get("datatype", None)
                    if datatype:
                        deequ_datatype = getattr(ConstrainableDataTypes, datatype, None)
                        if deequ_datatype is None:
                            raise ValueError(f"Invalid data type '{datatype}'")
                        constrainedCheck = method(
                            column, deequ_datatype, assertion=assertion_func, hint=hint
                        )

                elif assertion:
                    # Safely evaluate the assertion (e.g., lambda)
                    if column or columns:
                        if kwargs:
                            constrainedCheck = method(
                                column or columns,
                                assertion=assertion_func,
                                hint=hint,
                                **kwargs,
                            )
                        else:
                            constrainedCheck = method(
                                column or columns, assertion=assertion_func, hint=hint
                            )
                    else:
                        if kwargs:
                            constrainedCheck = method(
                                assertion=assertion_func, hint=hint, **kwargs
                            )
                        else:
                            constrainedCheck = method(
                                assertion=assertion_func, hint=hint
                            )
                elif column or columns:
                    # Apply constraint without assertion
                    if kwargs:
                        constrainedCheck = method(
                            column or columns, hint=hint, **kwargs
                        )
                    else:
                        constrainedCheck = method(column or columns, hint=hint)
                elif kwargs:
                    constrainedCheck = method(**kwargs)
                else:
                    constrainedCheck = method()
                if self._single_check_mode:
                    check = constrainedCheck
                else:
                    check_list.append(constrainedCheck)
            else:
                raise AttributeError(f"Deequ method '{constraint}' not found.")

        # Evidence is attributed by label, so two checks may not share one.
        checkspec.require_unique_labels(self.labels())
        if not self._order:
            raise ValueError("A deequ rule needs at least one check")

        # Process All daisy chained constraints via single or multi check approach in verification run builder
        if self._single_check_mode:
            verification_run_builder.addCheck(check)
        else:
            for check in check_list:
                verification_run_builder.addCheck(check)

        return verification_run_builder
