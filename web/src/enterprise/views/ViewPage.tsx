import React, { useMemo, useState, useEffect } from 'react'
import { ExtensionsControllerProps } from '../../../../shared/src/extensions/controller'
import { useView } from './useView'
import { ViewForm } from './forms/ViewForm'
import { Markdown } from '../../../../shared/src/components/Markdown'
import { renderMarkdown } from '../../../../shared/src/util/markdown'
import { MarkupKind } from '@sourcegraph/extension-api-classes'

interface Props extends ExtensionsControllerProps<'services'> {
    viewID: string
}

/**
 * A page that displays a single view (contributed by an extension).
 */
export const ViewPage: React.FunctionComponent<Props> = ({ viewID, extensionsController }) => {
    const data = useView(
        viewID,
        useMemo(() => extensionsController.services.contribution.getContributions(), [
            extensionsController.services.contribution,
        ]),
        extensionsController.services.view
    )

    // Wait for extensions to load for up to 5 seconds before showing "not found".
    const [waited, setWaited] = useState(false)
    useEffect(() => {
        setTimeout(() => setWaited(true), 5000)
    }, [])

    if (data === undefined || (!waited && data === null)) {
        return null
    }
    if (data === null) {
        return (
            <div className="alert alert-danger">
                View not found: <code>{viewID}</code>
            </div>
        )
    }

    const { contribution, form, view } = data
    return (
        <div>
            <h1>{contribution.title !== undefined ? contribution.title : contribution.id}</h1>
            {form === undefined ? null : form === null ? (
                <div className="alert alert-danger">
                    View form not found: <code>{contribution.form}</code>
                </div>
            ) : (
                <ViewForm form={form} extensionsController={extensionsController} />
            )}
            {view?.content.map((content, i) => (
                <section key={i} className="mt-3">
                    {content.kind === MarkupKind.Markdown ? (
                        <Markdown dangerousInnerHTML={renderMarkdown(content.value)} />
                    ) : (
                        content.value
                    )}
                </section>
            ))}
        </div>
    )
}
